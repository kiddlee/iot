name := "spark"

version := "1.0"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.11" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1",
  "mysql" % "mysql-connector-java" % "5.1.36",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.google.code.gson" % "gson" % "2.8.0",
  "com.alibaba" % "fastjson" % "1.2.17",
  "org.apache.hbase" % "hbase-client" % "1.2.4",
  "org.apache.hbase" % "hbase-common" % "1.2.4",
  "org.apache.hbase" % "hbase-server" % "1.2.4"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "unused$UnusedStubClass.class" => MergeStrategy.first
  case _ => MergeStrategy.first
}