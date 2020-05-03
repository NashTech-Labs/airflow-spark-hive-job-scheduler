name := "BigDataProject"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.18"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.4"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "8.2.0.jre8"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"
libraryDependencies += "io.delta" %% "delta-core" % "0.5.0"
