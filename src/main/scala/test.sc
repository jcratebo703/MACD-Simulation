import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable._
import scala.util.Random
//import scala.collection.mutable.ArrayBuffer
//
//val longestDays: Array[Int] = Array(75, 76, 75)
//
//val skipDays: Int = (3.45*(longestDays(1)+1)).ceil.toInt+(3.45*(longestDays(2)+1)).ceil.toInt-2//OMG
//println("skipDays: " + skipDays)
//
//println("test: " + (longestDays(1)+1)*3.45.ceil)
//
//var test0 = ArrayBuffer[Int]()
//for(i <- 1 to 5) test0 += i
////test0.transform(_+1)
//
//test0.foreach(x => println("\n"+ x))
//
//var cumulativeRate: Double = 1
//test0.foreach(x => cumulativeRate *= x)
//
//println(cumulativeRate)
val spark = SparkSession.builder()
  .appName("GitHub push counter")
  .master("local[*]")
  .getOrCreate()

val sc = spark.sparkContext
val df = spark.read.csv("/Users/caichengyun/Documents/User/CGU/Subject/畢專/CSV/2330 台積電.csv")

val dfInverse = df.orderBy("_c0")// the date of data must be ascending (r0=2019/01/02 r1=2019/01/01)
dfInverse.show()
val rows: Array[Row] = dfInverse.collect()

val closeArr: Array[Double] = rows.map(_.getString(4)).map(_.toDouble)
val closeRDD = sc.parallelize(closeArr)

val closeSum: Double = closeRDD.sum()
val closeAvg: Double = closeRDD.mean()
val closeNum: Double = closeRDD.count()

println("closeNum: " + closeNum + "\ncloseAvg: " + closeAvg + "\ncloseSum: " + closeSum)