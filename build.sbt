name := "SparkProject_01"

version := "1.0"

scalaVersion := "2.11.8"

val SparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion ,
  "org.apache.spark" %% "spark-sql" % SparkVersion 
)

libraryDependencies += "com.crealytics" %% "spark-excel" % "0.11.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"