import org.apache.spark.sql._
import  scala.collection.mutable._
import  scala.math._
import org.apache.spark.sql.functions.unix_timestamp

//case class MACDtest(id: Int, desc: String)

object MACD_test_0201 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    val df = spark.read.csv("/Users/caichengyun/Documents/User/CGU/Subject/畢專/CSV/2330 台積電.csv")

    df.show(100)

    //val dfInverse = df.orderBy("_c0")// date of the file must be ascending (r0=2019/01/02 r1=2019/01/01)

    //val dfInverse = df.sort(unix_timestamp($"_c0", "yyyy/mm/dd"))

    val pattern = "yyyy/MM/dd"

    val dfInverse = df
      .withColumn("timestampCol", unix_timestamp(df("_c0"), pattern).cast("timestamp"))
      .orderBy("timestampCol")

    dfInverse.show(100)

    val rows: Array[Row] = dfInverse.collect()

    val closeArr: Array[Double] = rows.map(_.getString(4)).map(_.toDouble)
    val closeRDD = sc.parallelize(closeArr)

    val closeSum: Double = closeRDD.sum()
    val closeAvg: Double = closeRDD.mean()
    val closeNum: Double = closeRDD.count()

    println("closeNum: " + closeNum + "\ncloseAvg: " + closeAvg + "\ncloseSum: " + closeSum)

    val closeWithIndex = closeRDD.zipWithIndex()
    val indexCloseRDD = closeWithIndex.map{case (k, v) => (v, k)}
    val emaCloseAryBuf = ArrayBuffer[Double]()

    val indexCloseMap = indexCloseRDD.collect().toMap

//    //val alpha = Array(12.0, 26.0, 9.0)
//    val Ema = (index: Int) => {
//      val alpha: Double = 2.0 / (index + 1.0)
//      val Nday = (3.45 * (index + 1)).ceil.toInt
//      val emaAryBuffer = ArrayBuffer[Double]()
//      var buf: Double = 0
//
//      for(j <- indexCloseMap.size-1 to 0 by -1){
//
//        if(j <= indexCloseMap.size-Nday) {
//          for (i <- j - (Nday - 1) to j) {
//           // emaAryBuffer += indexCloseRDD.lookup(i).toArray.mkString("").toDouble
//            emaAryBuffer += indexCloseMap.get(i).toArray.mkString("").toDouble
//            //var buf: Double = 0
//          }
//
//          for(k <- 0 to emaAryBuffer.length-1){
//            buf += emaAryBuffer((k-(emaAryBuffer.length-1)).abs) * pow(1 - alpha, k)
//
//          }
//          emaCloseAryBuf += buf * alpha
//          //println(emaCloseAryBuf)
//          buf = 0
//          emaAryBuffer.clear()
//
//        }else{
//            emaCloseAryBuf += indexCloseMap.get(j).toArray.mkString("").toDouble
//        }
//        //println(j)
//      }
//      emaCloseAryBuf.foreach(println)
//      println("\nemaCloseAryBuf SIZE :" + emaCloseAryBuf.size)
//    }

//    println("45456645645646456464")
    //Ema(12)
    //val macdAryBuf = for(i <- 0 until

  }

}

