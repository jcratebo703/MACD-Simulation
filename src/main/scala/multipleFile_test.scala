import java.io.File

import scala.util.control.Breaks._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import breeze.linalg._
import breeze.numerics._

import scala.collection.mutable.ArrayBuffer
import java.sql.{Connection, DriverManager, PreparedStatement}


object multipleFile_test extends App{
      val url = "jdbc:mysql://localhost:3306/mysql"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "jcratebo703"
      var connection: Connection = _

      val spark = SparkSession.builder()
        .appName("GitHub push counter")
        .master("local[*]")
        .getOrCreate()

      val sc = spark.sparkContext
//      val df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")
//      val dfInverse = df.orderBy("_c0")// the date of data must be ascending (r0=2019/01/02 r1=2019/01/01)
      val path: String = "/Users/caichengyun/Documents/User/CGU/Subject/三下/Financial Management/FinalProject/Final Project Data/"

      val Schema = StructType(Array(
        StructField("Date", StringType, nullable = false),
        StructField("Close", DoubleType, nullable = false),
        StructField("Open", DoubleType, nullable = false),
        StructField("Highest", DoubleType, nullable = false),
        StructField("Lowest", DoubleType, nullable = false),
        StructField("id", IntegerType, nullable = false)))

      def readExcel(file: String): DataFrame = spark.read
        .format("com.crealytics.spark.excel")
        .schema(Schema) // Optional, default: Either inferred schema, or all columns are Strings
        //      .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
        .option("useHeader", "false") // Required
        .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
        .option("inferSchema", "false") // Optional, default: false
        .option("addColorColumns", "false") // Optional, default: false
        .option("timestampFormat", "yyyy/mm/dd HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
        //      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
        //      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
        //      .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
        //      .option("location", path)
        .load(file)

      val dir = new File(path)
      val excelFiles = dir.listFiles.sorted.map(f => f.toString)  // Array[String]
      excelFiles.foreach(println)

      val dfs = excelFiles.map(f => readExcel(f).withColumn("id", monotonically_increasing_id())
        .filter(col("id") >= 2).orderBy("Date"))  // Array[DataFrame]  .orderBy("Date")
      //val ppdf = dfs.reduce(_.union(_))

      val trimFiles = excelFiles.map(x =>  x.replaceAll("[a-zA-z]|[0-9]|/|\\.| ", "")
        .replaceAll("^.{2}", ""))

      val typeOneCrashFile = ArrayBuffer[String]()
      val typeTwoCrashFile = ArrayBuffer[String]()
      val typeThreeCrashFile = ArrayBuffer[String]()

      val Ema = (index: Int, closeData: Map[Long, Double]) => {
        val alpha: Double = 2.0 / (index + 1.0)
        val Nday = (3.45 * (index + 1)).ceil.toInt
        val emaAryBuffer = ArrayBuffer[Double]()
        val emaCloseAryBuf = ArrayBuffer[Double]()
        var buf: Double = 0
        var T: Double = 0
        var P: Double = 0

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

      for(terms <- 0 to dfs.size-1){

        val rows: Array[Row] = dfs(terms).collect()

        val closeArr: Array[Double] = rows.map(_.getDouble(1))
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

        val indexCloseMap = indexCloseRDD.collect().toMap

        var index: Array[Int] = Array(12, 26, 9)

        //Ema(index(0)).foreach(println)

        val emaAryBuf1 = ArrayBuffer[Double]()
        emaAryBuf1 ++= Ema(index(0), indexCloseMap)
        //println("index0 : "+emaAryBuf1)
        val emaAryBuf2 = ArrayBuffer[Double]()
        emaAryBuf2 ++= Ema(index(1), indexCloseMap)
        //println("index1 : "+emaAryBuf2)
        println("\nCloseAryBuf SIZE :" + emaAryBuf1.size)

        val longestDay: Int = (3.45 * (index(1) + 1)).ceil.toInt

        val difAryBuf = ArrayBuffer[Double]()
        for(i <- longestDay-1 to emaAryBuf1.size-1){
          difAryBuf += emaAryBuf1(i) - emaAryBuf2(i)
        }
        //println("\nDIF: " + difAryBuf)
        println("\n DIF length: " + difAryBuf.size)


        val difMap = sc.parallelize(difAryBuf).zipWithIndex.map{case (k, v) => (v, k)}.collect().toMap

        val macdAryBuf = ArrayBuffer[Double]()
        macdAryBuf ++= Ema(index(2), difMap)
        //println("\nMACD: " + macdAryBuf)
        println( "\n MACD length: " + macdAryBuf.size)

        breakable{

          if(macdAryBuf.size <= 0){
            typeOneCrashFile += trimFiles(terms)
            break()
          }

          /* Simulation */


          var Buf: Double = -1
          val sellIndex = ArrayBuffer[Int]()
          val buyIndex = ArrayBuffer[Int]()
          val sellPrice = ArrayBuffer[Double]()
          val buyPrice = ArrayBuffer[Double]()
          val priceDif = ArrayBuffer[Double]()
          val returnRate = ArrayBuffer[Double]()
          var b, s: Int = 0

          for(i <- 0 to macdAryBuf.size-2){
            val preHis =  difAryBuf(i) - macdAryBuf(i)
            val postHis = difAryBuf(i+1) - macdAryBuf(i+1)
            val close: Double = indexCloseMap.get(i+longestDay).toArray.mkString("").toDouble //i+1+longestDay-1
            //var spend = asset * transRatio
            //var sellNumb = stockNum * transRatio

            if(preHis < 0 && postHis > 0){ //negative to positive, buy
              if(postHis/preHis > 1.1 && postHis/postHis > 0.9){// should use close price
                typeThreeCrashFile += trimFiles(terms)
              }
              else{
                Buf = close
                b += 1
                buyPrice += close
                //stockNum += spend / indexCloseMap.get(i).toArray.mkString("").toDouble
                //asset -= spend
                buyIndex += i+1
              }
            }
            else if(preHis > 0 && postHis < 0){
              s += 1
              //asset += indexCloseMap.get(i).toArray.mkString("").toDouble * sellNumb
              //stockNum -= sellNumb
              if(Buf != -1){
                sellIndex += i+1
                priceDif += Buf - close
                returnRate += (close-Buf)/Buf
                sellPrice += close

                Buf = -1
              }
            }
          }

          if(sellIndex.size != buyIndex.size || sellIndex.size != returnRate.size){
            buyIndex.remove(buyIndex.size-1)
            buyPrice.remove(buyPrice.size-1)
            //println("\n error occur!!!!!")
          }

          if(buyIndex.size <= 0){
            typeTwoCrashFile += trimFiles(terms)
            break()
          }

          val firstBuy: Double = indexCloseMap.get(buyIndex(0)+longestDay-1).toArray.mkString("").toDouble
          val lastSell: Double = indexCloseMap.get(sellIndex(sellIndex.size-1)+longestDay-1).toArray.mkString("").toDouble
          val cRate: Double = (lastSell-firstBuy)/firstBuy

          val count = returnRate.size
          val mean = returnRate.sum/count
          val devs = returnRate.map(x => pow(x - mean, 2))
          val stddev = sqrt(devs.sum / (count - 1))
          val probability = 1-(0.5 * (1 + erf((0 - mean) / stddev * sqrt(2))))

          println("\n Sell Index: " + sellIndex + "\n Sell counts: " + sellIndex.size)
          println("\n Buy Index: " + buyIndex + "\n Buy counts: " + buyIndex.size)
          println("\n Sell Price: " + sellPrice)
          println("\n Buy Price: " + buyPrice)
          println("\n Rate of Return: " + returnRate)
          println("\n Maximum: " + returnRate.max)
          println("\n Minimum: " + returnRate.min)
          println("\n Cumulative Return Rate: " + cRate)
          println("\n Expectation of Return Rate: " + mean)
          println("\n Standard Deviation: " + stddev)
          println("\n Probability: " + probability)
          println("\n Terms: " + (terms+1))
          println("\n File number: " + dfs.size)

          println("\n b : " + b + "\n s : " + s)

          println("\n Simulation complete")

          //MySQL connection
          try {
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)

            val insertSQL = "INSERT INTO scalaTest.cop (Name, TranFrequency, CRate, ERate, STDDEV, Probability, max, min) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"

            val prep: PreparedStatement = connection.prepareStatement(insertSQL)

            prep.setString(1, trimFiles(terms))
            prep.setInt(2, count)
            prep.setDouble(3, cRate)
            prep.setDouble(4, mean)
            prep.setDouble(5, stddev)
            prep.setDouble(6, probability)
            prep.setDouble(7, returnRate.max)
            prep.setDouble(8, returnRate.min)
            prep.execute()

            prep.close()

          } catch {
            case e: Exception => e.printStackTrace
          }
          connection.close
        }
      }
      typeTwoCrashFile.foreach(x => println("\n File : " + x + " don't have any transaction!!"))
      typeOneCrashFile.foreach(x => println("\n File : " + x + " don't have enough data!!"))
      typeThreeCrashFile.foreach(x => println("\n File : " + x + "have certain close price error!!"))
}
