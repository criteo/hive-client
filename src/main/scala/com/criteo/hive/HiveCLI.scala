package com.criteo.hive

import org.apache.hive.service.cli.thrift._
import org.jline.reader.{EndOfFileException, LineReaderBuilder, UserInterruptException}
import org.jline.terminal.TerminalBuilder
import org.rogach.scallop.ScallopConf

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object HiveCLI {

  def main(args: Array[String]): Unit = {
    val conf = new HiveCLIConf(args)
    val cli = new HiveCLI(HiveUserPass(conf.username(), conf.password()))
    conf.sql.map(cli.executeQuery).getOrElse(cli.interactive())
  }

}

class HiveCLI(credentials: HiveCredentials) {

  def interactive(): Unit = {
    val term = TerminalBuilder.terminal()
    val lineReader = LineReaderBuilder.builder().terminal(term).build()

    while (true) {
      try {
        val line = lineReader.readLine("hive> ")
        if (line.length > 0) {
          executeQuery(sql = line)
        }
      } catch {
        case uie: UserInterruptException => println(s"caught interrupt: $uie")
        case eof: EndOfFileException => println("exiting!"); return
      }
    }
  }

  def executeQuery(sql: String): Unit =
    new HiveThriftDriver("127.0.0.1", 10000)
      .executeQuery(sql, credentials)
      .map {
        case (Some(res), Some(md)) => printResults(res.getResults, md.getSchema)
        case _ => println("SUCCESS")
      }.andThen {
      case Failure(t) => println(s"FATAL ERROR: $t")
      case Success(_) => println("====> OK!")
    }

  def printResults(rows: TRowSet, schema: TTableSchema): Unit = {
    val rowsIter = HiveThriftDriver.resultsToIterable(rows)
    if (rowsIter.nonEmpty) {
      val widths = new Array[Int](rowsIter.head.size)
      val header = parseLine(schema.columns.asScala.map(_.columnName.split("\\.").last), widths)
      val lines = rowsIter.map(parseLine(_, widths))
      val format = mkFormatter(widths, "  ")
      val separator = widths.toList.map { len => "-" * len }

      (Seq(header, separator) ++ lines).foreach { line => println(format.format(line: _*)) }
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
}

class HiveCLIConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val username = opt[String](required = true)
  val password = opt[String](required = true)
  val sql = trailArg[String](required = false)
  verify()
}
