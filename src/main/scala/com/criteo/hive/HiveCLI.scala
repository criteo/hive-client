package com.criteo.hive

import hive.service.thrift.{TRowSet, TTableSchema}
import org.jline.reader.{EndOfFileException, LineReaderBuilder, UserInterruptException}
import org.jline.terminal.TerminalBuilder
import org.rogach.scallop.ScallopConf

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object HiveCLI {

  class PrintLnLogger extends HiveClientLogger {
    override def info(line: => String): Unit = println(line)
    override def error(line: => String): Unit = println(line)
    override def debug(line: => String): Unit = println(line)
  }

  def main(args: Array[String]): Unit = {
    val conf = new HiveCLIConf(args)

    val cli = if (conf.hiveprincipal.isDefined) {
      new HiveCLI(HiveKerberos(
        hivePrincipal = conf.hiveprincipal(),
        principal = conf.principal.toOption,
        keytabFile = conf.keytabfile.toOption
      ),
        conf.host(), conf.port(), conf.database(), new PrintLnLogger)
    } else {
      new HiveCLI(
        HiveUserPass(conf.username(), conf.password()), conf.host(), conf.port(), conf.database(), new PrintLnLogger)
    }
    conf.sql.map(opt => {
      try {
        Await.result(cli.executeQuery(opt, conf.database()), Duration.Inf)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          System.exit(1)

      }
    }).getOrElse(cli.interactive())
  }
}

class HiveCLI(credentials: HiveCredentials,
              host: String,
              port: Integer,
              database: String,
              hiveClientLogger: HiveClientLogger) {

  def interactive(): Unit = {
    val term = TerminalBuilder.terminal()
    val lineReader = LineReaderBuilder.builder().terminal(term).build()

    while (true) {
      try {
        val line = lineReader.readLine("hive> ")
        if (line.length > 0) {
          executeQuery(sql = line, database = database)
        }
      } catch {
        case uie: UserInterruptException => println(s"caught interrupt: $uie")
        case eof: EndOfFileException => println("exiting!"); return
      }
    }
  }

  def executeQuery(sql: String,
                   database: String,
                   hiveConf: Map[String, String] = Map.empty,
                   hiveVar: Map[String, String] = Map.empty): Future[Unit] = {
    def queryLog = (lines: Iterable[Seq[Any]]) =>
      lines.foreach(l => hiveClientLogger.debug(l.mkString(" ")))

    new HiveThriftDriver(host, port, hiveClientLogger)
      .executeQuery(sql = sql,
        database = database,
        credentials = credentials,
        hiveConf = hiveConf,
        hiveVar = hiveVar,
        logger = queryLog)
      .map {
        case (Some(res), Some(md)) => printResults(res.getResults, md.getSchema)
        case _ => ()
      }.andThen {
      case Failure(t) => hiveClientLogger.error(s"FATAL ERROR: $t")
      case Success(_) => hiveClientLogger.debug("SUCCESS")
    }
  }

  def printResults(rows: TRowSet, schema: TTableSchema): Unit = {
    HiveThriftDriver.parseResults(rows, schema).foreach(str => hiveClientLogger.info(str))
  }
}

class HiveCLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val username = opt[String]()
  val password = opt[String]()
  codependent(username, password)
  val hiveprincipal = opt[String](required = false)
  val keytabfile = opt[String](required = false)

  val principal = opt[String]()

  requireOne(username, hiveprincipal)
  requireOne(password, hiveprincipal)

  val host = opt[String](required=true)
  val port = opt[Int](required=true)

  val database = opt[String](required=false, default=Some("default"))

  val sql = trailArg[String](required = false)
  verify()
}
