package com.criteo.hive

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.hive.service.cli.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import scala.concurrent.{ExecutionContext, Future, Promise}

class HiveThriftDriver(addr: String, port: Int)(implicit ec: ExecutionContext) {

  import scala.collection.JavaConverters._

  private val transport = new TSocket(addr, port)
  transport.open()

  private val protocol = new TBinaryProtocol(transport)
  private val cliService = new TCLIService.Client(protocol)

  type QueryResults = (Option[TFetchResultsResp], Option[TGetResultSetMetadataResp])

  def executeQuery(sql: String, credentials: HiveCredentials, maxRows: Int = 20, logger: Iterable[Seq[Any]] => Unit = HiveThriftDriver.log): Future[QueryResults] =
    openSession(credentials).flatMap { session =>
      val fExec = for {
        execution <- executeStatement(sql, session.getSessionHandle)
        _ <- logOperation(execution.getOperationHandle)(logger)
        results <- fetchResults(execution.getOperationHandle, maxRows)
        metadata <- metaData(execution.getOperationHandle)
      } yield (results, metadata)

      fExec.andThen { case _ => closeSession(session.getSessionHandle) }
    }

  def openSession(credentials: HiveCredentials): Future[TOpenSessionResp] = Future {
    val sessReq = new TOpenSessionReq
    credentials match {
      case HiveUserPass(user, pass) =>
        sessReq.setUsername(user)
        sessReq.setPassword(pass)
      case HiveKerberos(principal) =>
        // ref: https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-UsingKerberoswithaPre-AuthenticatedSubject
        sessReq.setConfiguration(
          Map(
            "principal" -> principal,
            "kerberosAuthType" -> "fromSubject"
          ).asJava
        )
    }
    cliService.OpenSession(sessReq)
  }

  def executeStatement(sql: String, sessionHandle: TSessionHandle): Future[TExecuteStatementResp] = Future {
    val execReq = new TExecuteStatementReq
    execReq.setSessionHandle(sessionHandle)
    execReq.setConfOverlay(Map.empty[String, String].asJava)
    execReq.setRunAsync(true)
    execReq.setStatement(sql)
    val resp = cliService.ExecuteStatement(execReq)
    checkStatus(resp.getStatus)
    resp
  }

  def operationStatus(operationHandle: TOperationHandle): Future[TGetOperationStatusResp] = Future {
    val req = new TGetOperationStatusReq()
    req.setOperationHandle(operationHandle)
    val resp = cliService.GetOperationStatus(req)
    checkStatus(resp.getStatus)
    resp
  }

  def closeOperation(operationHandle: TOperationHandle): Future[TCloseOperationResp] = Future {
    val req = new TCloseOperationReq()
    req.setOperationHandle(operationHandle)
    val resp = cliService.CloseOperation(req)
    checkStatus(resp.getStatus)
    resp
  }

  def closeSession(sessionHandle: TSessionHandle): Future[TCloseSessionResp] = Future {
    val req = new TCloseSessionReq()
    req.setSessionHandle(sessionHandle)
    val resp = cliService.CloseSession(req)
    checkStatus(resp.getStatus)
    resp
  }

  def logOperation(operationHandle: TOperationHandle, lastCall: Boolean = false)(outFn: Iterable[Seq[Any]] => Unit): Future[Unit] = Future {
    val logReq = new TFetchResultsReq()
    logReq.setOperationHandle(operationHandle)
    logReq.setOrientation(TFetchOrientation.FETCH_NEXT)
    logReq.setMaxRows(1000)
    logReq.setFetchType(1)

    val resp = cliService.FetchResults(logReq)
    checkStatus(resp.getStatus)
    outFn(HiveThriftDriver.resultsToIterable(resp.getResults))
  }.flatMap(u => operationFinished(operationHandle)).flatMap {
    case true if lastCall => Future.successful()
    case finished => HiveThriftDriver.timer(500).flatMap(u => logOperation(operationHandle, finished)(outFn))
  }

  def operationFinished(operationHandle: TOperationHandle): Future[Boolean] = Future {
    val resp = cliService.GetOperationStatus(new TGetOperationStatusReq(operationHandle))
    checkStatus(resp.getStatus)
    TOperationState.FINISHED_STATE.getValue == resp.getOperationState.getValue
  }

  def metaData(operationHandle: TOperationHandle): Future[Option[TGetResultSetMetadataResp]] = Future {
    if (operationHandle.isHasResultSet) {
      val req = new TGetResultSetMetadataReq()
      req.setOperationHandle(operationHandle)
      val resp = cliService.GetResultSetMetadata(req)
      checkStatus(resp.getStatus)
      Some(resp)
    } else {
      None
    }
  }

  def fetchResults(operationHandle: TOperationHandle, maxRows: Int): Future[Option[TFetchResultsResp]] =
    if (operationHandle.isHasResultSet) {
      operationFinished(operationHandle).flatMap {
        case true => Future {
          val req = new TFetchResultsReq()
          req.setOperationHandle(operationHandle)
          req.setOrientation(TFetchOrientation.FETCH_NEXT)
          req.setMaxRows(maxRows)
          req.setFetchType(0)
          val resp = cliService.FetchResults(req)
          checkStatus(resp.getStatus)
          Some(resp)
        }
        case false => HiveThriftDriver.timer(500).flatMap(u => fetchResults(operationHandle, maxRows))
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

final case class HiveKerberos(principal: String) extends HiveCredentials

object HiveThriftDriver {

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

  def timer(delayMs: Long): Future[Unit] = {
    val p = Promise[Unit]
    Executors
      .newSingleThreadScheduledExecutor()
      .schedule(new Runnable {
        def run() = p.success(Unit)
      }, delayMs, TimeUnit.MILLISECONDS)
    p.future
  }

  def log(lines: Iterable[Seq[Any]]): Unit = lines.foreach(t => println(t.mkString(" : ")))

}
