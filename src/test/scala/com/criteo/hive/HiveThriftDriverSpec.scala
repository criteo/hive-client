package com.criteo.hive

import java.net.ServerSocket

import com.criteo.hive.HiveCLI.PrintLnLogger
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import hive.service.thrift.TStatusCode
import org.apache.hive.service.server.HiveServer2
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}


class HiveThriftDriverSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll {

  // the below is mostly stolen from:
  // https://github.com/apache/hive/blob/release-1.1.0/service/src/test/org/apache/hive/service/cli/thrift/ThriftCLIServiceTest.java
  // and
  // https://github.com/apache/hive/blob/release-1.1.0/itests/hive-unit/src/test/java/org/apache/hive/service/cli/thrift/TestThriftBinaryCLIService.java
  val socket = new ServerSocket(0)
  val port = socket.getLocalPort
  socket.close()

  val hiveServer2 = new HiveServer2()
  val hiveConf = new HiveConf

  hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false)
  hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "127.0.0.1")
  hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port)
  hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "NOSASL") // note: NONE breaks tests with our client
  hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary")
  hiveConf.setVar(ConfVars.METASTORECONNECTURLKEY, "jdbc:derby:memory:metastore_db;create=true;createDatabaseIfNotExist=true")

  hiveServer2.init(hiveConf)
  hiveServer2.start()
  Thread.sleep(500) // not my idea--stolen as above from ThriftCLIServiceTest

  override def afterAll(): Unit = {
    // this apparently doesn't clean everything up as repeated runs in the same sbt session will start throwing
    // OoM metaspace execeptions
    hiveServer2.stop()
  }

  "A HiveThriftDriverSpec" should {
    "connect" in {
      val driver = new HiveThriftDriver("127.0.0.1", port, new PrintLnLogger())
      driver.openSession(HiveUserPass("foo", "bar"),
        database = "default",
        hiveConf = Map[String, String](),
        hiveVar = Map[String, String]()).map { r =>
        Option(r.getSessionHandle).map(driver.closeSession)
        TStatusCode.SUCCESS_STATUS shouldEqual r.status.statusCode
      }
    }
    "execute a query" in {
      new HiveThriftDriver("127.0.0.1", port, new PrintLnLogger())
        .executeQuery(
          sql = "select 1",
          database = "default",
          credentials = HiveUserPass("foo", "bar"),
          hiveConf = Map[String, String](),
          hiveVar = Map[String, String]())
        .map {
          case (Some(res), _) => res.results.columns.get(0).getI32Val.values.get(0) shouldBe 1
          case _ => fail("no results received!")
        }
    }
  }

}
