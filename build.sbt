name := "hive-client"
organization := "com.criteo"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.11"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.10.0",
  "org.rogach" %% "scallop" % "3.0.3",
  "org.jline" % "jline" % "3.3.0",

  "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.6.5" % "test",
  "org.apache.hadoop" % "hadoop-common" % "2.6.5" % "test",
  "org.apache.hive" % "hive-jdbc" % "1.1.0" % "test",
  "org.apache.hive" % "hive-service" % "1.1.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
