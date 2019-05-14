import org.apache.spark.sql._
import  scala.collection.mutable._
import  scala.math._

case class MACDtest(id: Int, desc: String)

object MACD_test_0201 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")

    val rows: Array[Row] = df.collect()

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

    //val alpha = Array(12.0, 26.0, 9.0)
    val Ema = (index: Int) => {
      val alpha: Double = 2.0 / (index + 1.0)
      val Nday = (3.45 * (index + 1)).ceil.toInt
      val emaAryBuffer = ArrayBuffer[Double]()
      var buf: Double = 0

      for(j <- 0 until  indexCloseMap.size-1){

        if(j-(Nday-1)>= 0) {
          for (i <- j - (Nday - 1) to j) {
           // emaAryBuffer += indexCloseRDD.lookup(i).toArray.mkString("").toDouble
            emaAryBuffer += indexCloseMap.get(i).toArray.mkString("").toDouble
            //var buf: Double = 0
          }

          for(k <- 0 to emaAryBuffer.length-1){
            buf += emaAryBuffer((k-(emaAryBuffer.length-1)).abs) * pow(1 - alpha, k)

          }
          emaCloseAryBuf += buf * alpha
          //println(emaCloseAryBuf)
          buf = 0
          emaAryBuffer.clear()

        }else{
            emaCloseAryBuf += indexCloseMap.get(j).toArray.mkString("").toDouble
        }
        //println(j)
      }
      emaCloseAryBuf.foreach(println)
      println("\nemaCloseAryBuf SIZE :" + emaCloseAryBuf.size)
    }

//    println("45456645645646456464")
    Ema(12)
    //val macdAryBuf = for(i <- 0 until

  }

}

