import org.apache.spark.sql._
import  scala.collection.mutable._
import  scala.math._

val spark = SparkSession.builder()
  .appName("GitHub push counter")
  .master("local[*]")
  .getOrCreate()

val sc = spark.sparkContext

//def readExcel(file: String): DataFrame = spark.sqlContext.read
//  .format("com.crealytics.spark.excel")
//  .option("location", file)
//  .option("useHeader", "true")
//  .option("treatEmptyValuesAsNulls", "true")
//  .option("inferSchema", "true")
//  .option("addColorColumns", "False")
//  .load()
//
//val data = readExcel("path to your excel file")
//
//data.show(false)

var index: Array[Int] = Array(12, 26, 9)

index(0) = 1

index.foreach(print)

