import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.sql.functions.unix_timestamp


import scala.collection.immutable.ListMap

object OneFileOneParameter {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark.read.csv("/Users/caichengyun/Documents/User/CGU/Subject/畢專/CSV/2330 台積電.csv")

    //val dfInverse = df.orderBy("_c0")// the date of data must be ascending (r0=2019/01/02 r1=2019/01/01)

    val pattern = "yyyy/MM/dd"

    val dfInverse = df
      .withColumn("timestampCol", unix_timestamp(df("_c0"), pattern).cast("timestamp"))
      .orderBy("timestampCol")
    dfInverse.show()

    val rows: Array[Row] = dfInverse.collect()

    val closeArr: Array[Double] = rows.map(_.getString(4)).map(_.toDouble)
    val closeRDD = sc.parallelize(closeArr)

    val closeSum: Double = closeRDD.sum()
    val closeAvg: Double = closeRDD.mean()
    val closeNum: Double = closeRDD.count()
    //    for(i <- 0 to closeArr.length-1){
    //      //println(closeArr(i))
    //      closeSum += closeArr(i)
    //    }
    //    val closeAvg: Double = closeSum/closeArr.length
    println("closeNum: " + closeNum + "\ncloseAvg: " + closeAvg + "\ncloseSum: " + closeSum)
    //    df.printSchema()
    //    df.select("_c18").show()

    //    val macdRdd = closeRDD.map(x => x + 1000)
    //    macdRdd.foreach(x => println(x))

    val closeWithIndex = closeRDD.zipWithIndex()
    val indexCloseRDD = closeWithIndex.map{case (k, v) => (v, k)}
    val emaCloseAryBuf = ArrayBuffer[Double]()

    val indexCloseMap = indexCloseRDD.collect().toMap

    val index: Array[Int] = Array(12, 26, 9)

    val Ema = (index: Int, closeData: Map[Long, Double]) => {
      val alpha: Double = 2.0 / (index + 1.0)
      val Nday = (3.45 * (index + 1)).ceil.toInt
      val emaAryBuffer = ArrayBuffer[Double]()
      var buf: Double = 0
      var T: Double = 0
      var P: Double = 0

      emaCloseAryBuf.clear()

      for(j <- 0 to closeData.size-1){

        if(j - (Nday-1) >= 0) {

          if(j == Nday-1){
            for (i <- 0 to Nday-1) {
              // emaAryBuffer += indexCloseRDD.lookup(i).toArray.mkString("").toDouble
              emaAryBuffer += closeData.get(i).toArray.mkString("").toDouble
              //var buf: Double = 0
            }

            for(k <- 0 to emaAryBuffer.length-1){
              buf += emaAryBuffer((k-(emaAryBuffer.length-1)).abs) * pow(1 - alpha, k)
            }
            emaCloseAryBuf += buf * alpha
            buf = 0
          }
          else{
            T = alpha * closeData.get(j).toArray.mkString("").toDouble
            P = (1- alpha) * emaCloseAryBuf(emaCloseAryBuf.size-1)
            emaCloseAryBuf += T+P
          }
          //println(emaCloseAryBuf)
          //emaAryBuffer.clear()

        }else{
          emaCloseAryBuf += closeData.get(j).toArray.mkString("").toDouble
        }
        //println(j)
      }
      emaCloseAryBuf
    }
    //Ema(index(0)).foreach(println)


    val emaAryBuf1 = ArrayBuffer[Double]()
    emaAryBuf1 ++= Ema(index(0), indexCloseMap)
    //println("\nindex0 : "+emaAryBuf1)

    val emaAryBuf2 = ArrayBuffer[Double]()
    emaAryBuf2 ++= Ema(index(1), indexCloseMap)
    //println("\nindex1 : "+emaAryBuf2)

    println("\nCloseAryBuf SIZE :" + emaAryBuf1.size)

    val longestDay: Int = (3.45 * (index(1) + 1)).ceil.toInt

    val difAryBuf = ArrayBuffer[Double]()
    for(i <- longestDay-1 to emaAryBuf1.size-1){
      difAryBuf += emaAryBuf1(i) - emaAryBuf2(i)
    }
    println("\nDIF: " + difAryBuf)
    println("\n DIF length: " + difAryBuf.size)


    val difMap = sc.parallelize(difAryBuf).zipWithIndex.map{case (k, v) => (v, k)}.collect().toMap

    val macdAryBuf = ArrayBuffer[Double]()
    macdAryBuf ++= Ema(index(2), difMap)
    println("\nMACD: " + macdAryBuf)
    println( "\n MACD length: " + macdAryBuf.size)

    /* Simulation */

    var breakDaysMap: Map[String, Double] = Map()
    var maximumRateMap: Map[Double, Double] = Map()
    var expectationMap: Map[Double, Double] = Map()
    var frequencyMap: Map[Double, Double] = Map()
    val breakThresholdArybuf = ArrayBuffer[Double]()

    for(j <- 0 to 1) {

      val trans = new Transaction(macdAryBuf, difAryBuf, indexCloseMap, longestDay)

      trans.transSimul(j)
      trans.transFreqVerify()

      val threshold = trans.threshold
      val hasTrans = trans.testEmptyTrans()
      //val sellIndex = trans.sellIndex
      val transCounts = trans.transCount()

      println("count:" + transCounts)
      frequencyMap += (threshold -> transCounts)

      //Threshold has no transaction will break()
      breakable{
        if(hasTrans){
          breakThresholdArybuf += threshold
          break()
        }

        val returnRate = trans.returnRate

        val CRate = trans.calculateCum()
        val ERate = trans.calculateExp()
        val holdAndWait = trans.calculateHoldNWait()

        maximumRateMap += (threshold -> returnRate.max)
        expectationMap += (threshold -> ERate)

        breakDaysMap = breakDaysMap ++ trans.breakDaysMap


        println("\n Cumulative Return: " + CRate)
        println("\n Expected Return: " + ERate)
        println("\n Hold & Wait: " + holdAndWait)
        trans.resultsPrint()
      }
    }

    //presentation of the fourth parameter

    breakDaysMap = ListMap(breakDaysMap.toSeq.sortWith(_._1 < _._1):_*)
    println("\nBreak Days: ")
    breakDaysMap.foreach(println)

    maximumRateMap = ListMap(maximumRateMap.toSeq.sortWith(_._2 > _._2):_*)
    println("\nMaximum Rates: ")
    maximumRateMap.foreach(println)

    expectationMap = ListMap(expectationMap.toSeq.sortWith(_._2 > _._2):_*)
    println("\nExpectations: ")
    expectationMap.foreach(println)

    frequencyMap = ListMap(frequencyMap.toSeq.sortWith(_._2 > _._2):_*)
    println("\nFrequency: ")
    frequencyMap.foreach(println)

    println("\nThese thresholds have no transaction: ")
    breakThresholdArybuf.foreach(println)
  }
}
