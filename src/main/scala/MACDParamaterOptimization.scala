import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.math.pow

object MACDParamaterOptimization {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")
    val dfInverse = df.orderBy("_c0")// the date of data must be ascending (r0=2019/01/02 r1=2019/01/01)

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

    var index: Array[Int] = Array(12, 26, 9)


    val Ema = (index: Int, closeData: Map[Long, Double]) => {
      val alpha: Double = 2.0 / (index + 1.0)
      val Nday = (3.45 * (index + 1)).ceil.toInt
      val emaAryBuffer = ArrayBuffer[Double]()
      var buf: Double = 0
      var T: Double = 0
      var P: Double = 0

      emaCloseAryBuf.clear()

      for(j <- 0 to indexCloseMap.size-1){

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
            P = (1- alpha) * emaCloseAryBuf(emaAryBuffer.size-1)
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
    println("index0 : "+emaAryBuf1)
    val emaAryBuf2 = ArrayBuffer[Double]()
    emaAryBuf2 ++= Ema(index(1), indexCloseMap)
    println("index1 : "+emaAryBuf2)

    println("\nCloseAryBuf SIZE :" + emaAryBuf1.size)

    val difAryBuf = ArrayBuffer[Double]()
    for(i <- 0 to emaAryBuf1.size-1){
      difAryBuf += emaAryBuf1(i) - emaAryBuf2(i)
    }
    println("\nDIF: " + difAryBuf)

    val difMap = sc.parallelize(difAryBuf).zipWithIndex.map{case (k, v) => (v, k)}.collect().toMap

    val macdAryBuf = ArrayBuffer[Double]()
    macdAryBuf ++= Ema(index(2), difMap)
    println("\nMACD: " + macdAryBuf)

//    println(indexCloseMap.size)
//    println(macdAryBuf.size)
//    println(difAryBuf.size)
//    println(emaAryBuf1.size)

    /* Simulation */

    //val principal: Double = 1000000
    //val transRatio: Double = 0.1
    //var asset: Double = principal
    //var stockNum: Double = 0
    var Buf: Double = -1
    val sellIndex = ArrayBuffer[Int]()
    val buyIndex = ArrayBuffer[Int]()
    val priceDif = ArrayBuffer[Double]()
    val returnRate = ArrayBuffer[Double]()
    var b, s: Int = 0

    for(i <- 0 to macdAryBuf.size-2){
      val preHis =  difAryBuf(i) - macdAryBuf(i)
      val postHis = difAryBuf(i+1) - macdAryBuf(i+1)
      val close: Double = indexCloseMap.get(i+1).toArray.mkString("").toDouble
      //var spend = asset * transRatio
      //var sellNumb = stockNum * transRatio

      if(preHis < 0 && postHis > 0){ //negative to positive, buy
        Buf = close
        b += 1
        //stockNum += spend / indexCloseMap.get(i).toArray.mkString("").toDouble
        //asset -= spend
        buyIndex += i
      }
      else if(preHis > 0 && postHis < 0){
        s += 1
        //asset += indexCloseMap.get(i).toArray.mkString("").toDouble * sellNumb
        //stockNum -= sellNumb
        if(Buf != -1){
          sellIndex += i
          priceDif += Buf - close
          returnRate += (Buf - close)/Buf

          Buf = -1
        }
      }
    }

    if(sellIndex.size != buyIndex.size || sellIndex.size != returnRate.size){
      buyIndex.remove(buyIndex.size-1)
      //println("\n error occur!!!!!")
    }

    println("\n Rate of Return: " + returnRate)
    println("\n Expectation of Return Rate: " + returnRate.sum/returnRate.size)
    println("\n Sell Index:" + sellIndex + "\n Sell counts: " + sellIndex.size)
    println("\n Buy Index: " + buyIndex + "\n Buy counts: " + buyIndex.size)

    println("\n b : " + b + "\n s : " + s)

    println("\n Simulation complete")
  }

}
