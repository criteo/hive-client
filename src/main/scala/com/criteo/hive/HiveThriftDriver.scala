package com.criteo.hive

import java.time.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import hive.service.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._


class HiveThriftDriver(addr: String, port: Int, logger: HiveClientLogger)(implicit ec: ExecutionContext) {

  var transport: TTransport = null
  var protocol: TBinaryProtocol = null
  var cliService: TCLIService.Client = null

  type QueryResults = (Option[TFetchResultsResp], Option[TGetResultSetMetadataResp])

  /**
    * Executes a query on a new session and closes it.
    * @param sql query to execute
    * @param database default database
    * @param credentials authentication credentials
    * @param maxRows number of rows to return, no limit if unset
    * @param hiveConf hiveconf for the session
    * @param hiveVar hivevars for the session
    * @param logger logger to log messages for this query
    * @return result of the query
    */
  def executeQuery(sql: String,
                   database: String,
                   credentials: HiveCredentials,
                   maxRows: Int = Integer.MAX_VALUE,
                   hiveConf: Map[String, String],
                   hiveVar: Map[String, String],
                   logger: Iterable[Seq[Any]] => Unit = HiveThriftDriver.log
                  ): Future[QueryResults] =
    openSession(credentials, database, hiveConf, hiveVar).flatMap { session =>
      val fExec = for {
        execution <- executeStatement(sql, session.getSessionHandle)
        _ <- pollAndLogOperation(execution.getOperationHandle)(logger)
        results <- fetchResults(execution.getOperationHandle, maxRows)
        metadata <- metaData(execution.getOperationHandle)
      } yield (results, metadata)

      fExec.andThen { case _ => closeSession(session.getSessionHandle)
      }
    }


  /**
    * Executes a series of queries on a new Hive session, return the last result.
    * @param statements list of queries to run
    * @param database default database
    * @param credentials authentication credentials
    * @param maxRows number of rows to return, no limit if unset
    * @param hiveConf hiveconf for the session
    * @param hiveVar hivevars for the session
    * @param logger logger to log messages for this query
    * @return result of the last query in the list
    */
  def executeQueries(statements: List[String],
                     database: String,
                     credentials: HiveCredentials,
                     maxRows: Int = Integer.MAX_VALUE,
                     hiveConf: Map[String, String],
                     hiveVar: Map[String, String],
                     logger: Iterable[Seq[Any]] => Unit = HiveThriftDriver.log
                    ): Future[QueryResults] = {
    openSession(credentials, database, hiveConf, hiveVar).flatMap { session => {
      val finalFuture = statements.foldLeft(Future.successful[QueryResults]((None, None))) {
        case (previous, hiveql) => {
          previous.flatMap(_ =>
            for {
              execution <- executeStatement(hiveql, session.getSessionHandle)
              _ <- pollAndLogOperation(execution.getOperationHandle)(logger)
              results <- fetchResults(execution.getOperationHandle, maxRows)
              metadata <- metaData(execution.getOperationHandle)
            } yield (results, metadata)
          )
        }
      }
      finalFuture.andThen {
        case _ => closeSession(session.getSessionHandle)
      }
    }}
  }

  def openSession(credentials: HiveCredentials,
                  database: String,
                  hiveConf: Map[String, String],
                  hiveVar: Map[String, String]) : Future[TOpenSessionResp] = Future {
    synchronized {
      //UserGroupInformation uses static state to store the login user, so need to make sure it is not reset by another thread
      //until it is finished being used here to open the session.
      val sessReq = new TOpenSessionReq
      credentials match {
        case HiveUserPass(user, pass) => {
          sessReq.setUsername(user)
          sessReq.setPassword(pass)
          transport = new TSocket(addr, port)
        }
        case HiveKerberos(hivePrincipal, principal, keytabFile) => {
          // ref: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-UsingKerberoswithaPre-AuthenticatedSubject
          sessReq.setConfiguration(
            Map(
              "principal" -> hivePrincipal,
              "kerberosAuthType" -> "fromSubject"
            ).asJava
          )

          val saslProps = Map(
            "javax.security.sasl.qop" -> "auth-conf,auth-int,auth",
            "javax.security.sasl.server.authentication" -> "true")
          val conf = new Configuration()
          conf.set("hadoop.security.authentication", "kerberos")
          UserGroupInformation.setConfiguration(conf)
          if (principal.isDefined && keytabFile.isDefined) {
            UserGroupInformation.loginUserFromKeytab(principal.get, keytabFile.get)
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
            logger.debug(s"Logging in to $hivePrincipal from $principal using $keytabFile.")
          } else {
            logger.debug(s"Logging in to $hivePrincipal using ticket cache.  " +
              s"Warning, ticket will not be properly renewed, it is recommended to pass in keytabFile and service principal.")
          }
          val uTransport = new TSocket(addr, port)
          transport = SaslClient.createClientTransport(hivePrincipal, addr, "KERBEROS", null,
            uTransport, saslProps, UserGroupInformation.getLoginUser)
        }
      }
      val conf = new java.util.HashMap[String, String]
      hiveConf.foreach { case (key, value) => conf.put("set:hiveconf:" + key, value) }
      hiveVar.foreach { case (key, value) => conf.put("set:hivevar:" + key, value) }
      conf.put("use:database", database)
      sessReq.setConfiguration(conf)

      transport.open()
      protocol = new TBinaryProtocol(transport)
      cliService = new TCLIService.Client(protocol)
      val resp = cliService.synchronized {
        cliService.OpenSession(sessReq)
      }
      resp.status.statusCode match {
        case TStatusCode.ERROR_STATUS | TStatusCode.INVALID_HANDLE_STATUS => {
          val errorMessage = resp.status.errorMessage
          val errorStack = resp.status.infoMessages.asScala.mkString("\n\t")
          throw new RuntimeException(errorMessage + "\n\t" + errorStack)
        }
        case _ => resp
      }
    }
  }

  def executeStatement(sql: String, sessionHandle: TSessionHandle): Future[TExecuteStatementResp] = Future {
    val execReq = new TExecuteStatementReq
    execReq.setSessionHandle(sessionHandle)
    execReq.setConfOverlay(Map.empty[String, String].asJava)
    execReq.setRunAsync(true)
    execReq.setStatement(sql)
    val resp = cliService.synchronized {
      cliService.ExecuteStatement(execReq)
    }
    checkStatus(resp.getStatus)
    resp
  }

  def operationStatus(operationHandle: TOperationHandle): Future[TGetOperationStatusResp] = Future {
    val req = new TGetOperationStatusReq()
    req.setOperationHandle(operationHandle)
    val resp = cliService.synchronized {
      cliService.GetOperationStatus(req)
    }
    checkStatus(resp.getStatus)
    resp
  }

  def closeOperation(operationHandle: TOperationHandle): Future[TCloseOperationResp] = Future {
    val req = new TCloseOperationReq()
    req.setOperationHandle(operationHandle)
    val resp = cliService.synchronized {
      cliService.CloseOperation(req)
    }
    checkStatus(resp.getStatus)
    resp
  }

  def closeSession(sessionHandle: TSessionHandle): Future[TCloseSessionResp] = Future {
    val req = new TCloseSessionReq()
    req.setSessionHandle(sessionHandle)
    val resp = cliService.synchronized {
      cliService.CloseSession(req)
    }
    checkStatus(resp.getStatus)
    resp
  }

  def pollAndLogOperation(operationHandle: TOperationHandle)(outFn: Iterable[Seq[Any]] => Unit): Future[Unit] = Future {
    logOperation(operationHandle)(outFn)
  }.flatMap(u => getOperationStatus(operationHandle)).flatMap {
    case TOperationState.FINISHED_STATE => {
      logOperation(operationHandle)(outFn)
      Future.successful()
    }
    case TOperationState.ERROR_STATE => {
      logOperation(operationHandle)(outFn)
      Future.failed(new HiveException("Error, please check HiveServer2 logs"))
    }
    case TOperationState.CANCELED_STATE => {
      logOperation(operationHandle)(outFn)
      Future.failed(new HiveException("Error, job is cancelled."))
    }
    case _ => Utils.timeout(Duration.ofMillis(500)).flatMap(u => pollAndLogOperation(operationHandle)(outFn))
  }

  def logOperation(operationHandle: TOperationHandle)(outFn: Iterable[Seq[Any]] => Unit): Future[Unit] = Future {
    val logReq = new TFetchResultsReq()
    logReq.setOperationHandle(operationHandle)
    logReq.setOrientation(TFetchOrientation.FETCH_NEXT)
    logReq.setMaxRows(Integer.MAX_VALUE)
    logReq.setFetchType(1)
    val resp = cliService.synchronized {
      cliService.FetchResults(logReq)
    }
    checkStatus(resp.getStatus)
    outFn(HiveThriftDriver.resultsToIterable(resp.getResults))
  }

  def getOperationStatus(operationHandle: TOperationHandle): Future[TOperationState] = Future {
    val resp = cliService.synchronized {
      cliService.GetOperationStatus(new TGetOperationStatusReq(operationHandle))
    }
    checkStatus(resp.getStatus)
    resp.getOperationState
  }

  def metaData(operationHandle: TOperationHandle): Future[Option[TGetResultSetMetadataResp]] = Future {
    if (operationHandle.isHasResultSet) {
      val req = new TGetResultSetMetadataReq()
      req.setOperationHandle(operationHandle)
      val resp = cliService.synchronized {
        cliService.GetResultSetMetadata(req)
      }
      checkStatus(resp.getStatus)
      Some(resp)
    } else {
      None
    }
  }

  def fetchResults(operationHandle: TOperationHandle, maxRows: Int): Future[Option[TFetchResultsResp]] =
    if (operationHandle.isHasResultSet) {
      getOperationStatus(operationHandle).flatMap {
        case TOperationState.FINISHED_STATE => Future {
          val req = new TFetchResultsReq()
          req.setOperationHandle(operationHandle)
          req.setOrientation(TFetchOrientation.FETCH_NEXT)
          req.setMaxRows(maxRows)
          req.setFetchType(0)
          val resp = cliService.synchronized{
            cliService.FetchResults(req)
          }
          checkStatus(resp.getStatus)
          Some(resp)
        }
        case TOperationState.ERROR_STATE => Future { None }
        case TOperationState.CANCELED_STATE => Future { None }
        case _ => Utils.timeout(Duration.ofMillis(500)).flatMap(u => fetchResults(operationHandle, maxRows))
      }
    } else {
      Future.successful(None)
    }


  private def checkStatus(status: TStatus): Unit = if (TStatusCode.SUCCESS_STATUS.getValue != status.getStatusCode.getValue) {
    sys.error(status.getErrorMessage)
  }

}

sealed trait HiveCredentials

final case class HiveUserPass(username: String, password: String) extends HiveCredentials

final case class HiveKerberos(hivePrincipal: String,
                              principal: Option[String],
                              keytabFile: Option[String]) extends HiveCredentials

object HiveThriftDriver {

  def parseResults(rows: TRowSet, schema: TTableSchema): Seq[String] = {
    val rowsIter = HiveThriftDriver.resultsToIterable(rows)
    if (rowsIter.nonEmpty) {
      val widths = new Array[Int](rowsIter.head.size)
      val header = parseLine(schema.columns.asScala.map(_.columnName.split("\\.").last), widths)
      val lines = rowsIter.map(parseLine(_, widths))
      val format = mkFormatter(widths, "  ")
      val separator = widths.toList.map { len => "-" * len }

      (Seq(header, separator) ++ lines).map { line => format.format(line: _*) }
    } else {
      Seq[String]()
    }
  }

  def mkFormatter(widths: Array[Int], separator: String): String =
    widths.zipWithIndex.map { case (len, idx) => s"%${idx + 1}$$${len + 1}s" }.mkString(separator)

  def parseLine(line: Seq[Any], widths: Array[Int]): Seq[String] =
    line.zipWithIndex.map { case (v, i) =>
      val vStr = v.toString
      val vStrLen = vStr.length
      if (vStrLen > widths(i)) {
        widths(i) = vStrLen
      }
      vStr
    }

  def resultsToIterable(rows: TRowSet): Iterable[Seq[Any]] = {

    val columnSet = rows.getColumns

    // use the first column of the set as our row index generator
    val firstColumn = columnSet.get(0)

    val hasNextFn: (Int => Boolean) = firstColumn match {
      case x if x.isSetStringVal => (idx: Int) => x.getStringVal.getValues.size >= idx + 1
      case x if x.isSetI64Val => (idx: Int) => x.getI64Val.getValues.size >= idx + 1
      case x if x.isSetI32Val => (idx: Int) => x.getI32Val.getValues.size >= idx + 1
      case x if x.isSetI16Val => (idx: Int) => x.getI16Val.getValues.size >= idx + 1
      case x if x.isSetDoubleVal => (idx: Int) => x.getDoubleVal.getValues.size >= idx + 1
      case x if x.isSetByteVal => (idx: Int) => x.getByteVal.getValues.size >= idx + 1
      case x if x.isSetBoolVal => (idx: Int) => x.getBoolVal.getValues.size >= idx + 1
      case x if x.isSetBinaryVal => (idx: Int) => x.getBinaryVal.getValues.size >= idx + 1
      case _ => sys.error("unsupported value type!")
    }

    new Iterable[Seq[Any]] {
      private val colsIdx = 0 until columnSet.size

      override def iterator: Iterator[Seq[Any]] = new Iterator[Seq[Any]] {
        private var rowPos = 0

        override def hasNext: Boolean = hasNextFn(rowPos)

        override def next(): Seq[Any] = {
          val row = colsIdx.map { colPos =>
            columnSet.get(colPos) match {
              case x if x.isSetStringVal => x.getStringVal.getValues.get(rowPos)
              case x if x.isSetI64Val => x.getI64Val.getValues.get(rowPos)
              case x if x.isSetI32Val => x.getI32Val.getValues.get(rowPos)
              case x if x.isSetI16Val => x.getI16Val.getValues.get(rowPos)
              case x if x.isSetDoubleVal => x.getDoubleVal.getValues.get(rowPos)
              case x if x.isSetByteVal => x.getByteVal.getValues.get(rowPos)
              case x if x.isSetBoolVal => x.getBoolVal.getValues.get(rowPos)
              case x if x.isSetBinaryVal => x.getBinaryVal.getValues.get(rowPos)
              case _ => sys.error("unsupported value type!")
            }
          }
          rowPos = rowPos + 1
          row
        }
      }
    }

  }

  def log(lines: Iterable[Seq[Any]]): Unit = lines.foreach(t => println(t.mkString(" : ")))

}
