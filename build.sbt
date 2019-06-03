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

libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "0.13.2",

  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes. 
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % "0.13.2",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
)

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"