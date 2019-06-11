import org.apache.spark.sql._

import scala.collection.mutable._
import scala.util.Random
//import  scala.math._
//import scala.util.Random
//
//val spark = SparkSession.builder()
//  .appName("GitHub push counter")
//  .master("local[*]")
//  .getOrCreate()
//
//val sc = spark.sparkContext
//
////def readExcel(file: String): DataFrame = spark.sqlContext.read
////  .format("com.crealytics.spark.excel")
////  .option("location", file)
////  .option("useHeader", "true")
////  .option("treatEmptyValuesAsNulls", "true")
////  .option("inferSchema", "true")
////  .option("addColorColumns", "False")
////  .load()
////
////val data = readExcel("path to your excel file")
////
////data.show(false)
//
//var index: Array[Int] = Array(12, 26, 9)
//
//index(0) = 1
//
//index.foreach(print)
var randMin: Double = 2
val randMax: Double = 100
var d = Random.nextDouble()
var rand: Double = randMin + (randMax - randMin) * d
//
d = Random.nextDouble()
rand = randMin + (randMax - randMin) * d
println(d)
println(rand)

val rows: Int = 100
val cols: Int = 3

val a = Array.ofDim[Double](rows, cols)
//println(a(0).size)
//val sa = Array.fill(5)("Hi")
for(i <- 0 until a.size){
  d = Random.nextDouble()
  rand = randMin + (randMax - randMin) * d
  a(i)(0) = rand
  randMin = rand
  a(i)(1) = randMin + (randMax - randMin) * d
  d = Random.nextDouble()
  rand = randMin + (randMax - randMin) * d
  a(i)(2) = rand
  randMin = 2
}
a(99)


"1,2,3".split(",").map(_.toDouble)