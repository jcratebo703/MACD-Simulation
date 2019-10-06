import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement}

import breeze.numerics._
import multipleFile_test.{Ema, connection, dfs, driver, password, sc, trimFiles, typeOneCrashFile, typeThreeCrashFile, typeTwoCrashFile, url, username}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


object multipleFileOPT extends App{
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

  //EMA function
  val Ema = (index: Int, closeData: Map[Long, Double]) => {
    val alpha: Double = 2.0 / (index + 1.0)
    val Nday = (3.45 * (index + 1)).ceil.toInt
    val emaAryBuffer = ArrayBuffer[Double]()
    var buf: Double = 0
    var T: Double = 0
    var P: Double = 0
    val emaCloseAryBuf = ArrayBuffer[Double]()

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

  //iterate files
  for(terms <- 0 to dfs.size-1){

    val rows: Array[Row] = dfs(terms).collect()

    val closeArr: Array[Double] = rows.map(_.getDouble(1))
    val closeRDD = sc.parallelize(closeArr)

    val closeSum: Double = closeRDD.sum()
    val closeAvg: Double = closeRDD.mean()
    val closeNum: Double = closeRDD.count()

    println("closeNum: " + closeNum + "\ncloseAvg: " + closeAvg + "\ncloseSum: " + closeSum)

    val closeWithIndex = closeRDD.zipWithIndex()
    val indexCloseRDD = closeWithIndex.map{case (k, v) => (v, k)}

    val indexCloseMap = indexCloseRDD.collect().toMap

    val index: Array[Int] = Array(12, 26, 9)
    var opIndex: String = ""
    var opMap: Map[String, Double] = Map()

    //Ema(index(0)).foreach(println)

    var test: Int = 0
    val longestDays: Array[Int] = Array(30, 51, 50)
    val longest3rdNDay = (3.45*(longestDays(2)+1)).ceil.toInt

    val skipDays: Int = (3.45*(longestDays(1)+1)).ceil.toInt+ longest3rdNDay -2//OMG
    println("skipDays: " + skipDays)

    val sizeAry = ArrayBuffer[Int]()
    val transTime = ArrayBuffer[Int]()

    //Start para's OPT
    for(x <- longestDays(0) to 5 by -1){
      for(y <- longestDays(1) to x + 5 by -1){
        for(z <- longestDays(2) to 2 by -1){

          opIndex = x.toString + "," + y.toString + "," +z.toString

          index(0) = x
          index(1) = y
          index(2) = z

          val emaAryBuf1 = ArrayBuffer[Double]()
          emaAryBuf1 ++= Ema(index(0), indexCloseMap)
          //println("index0 : "+emaAryBuf1)
          val emaAryBuf2 = ArrayBuffer[Double]()
          emaAryBuf2 ++= Ema(index(1), indexCloseMap)
          //println("index1 : "+emaAryBuf2)
          println("\nCloseAryBuf SIZE :" + emaAryBuf1.size)

          val shortestTransDays: Int = emaAryBuf1.size-skipDays

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


          //equal all parameter's trans days
          //if(difAryBuf.size - longest3rdNDay + 1 > shortestTransDays){}
          val daysWillBeTrimmed = difAryBuf.size - shortestTransDays
          macdAryBuf.remove(0, daysWillBeTrimmed)
          difAryBuf.remove(0, daysWillBeTrimmed)

          sizeAry += macdAryBuf.size

          breakable{
            //            if(macdAryBuf.size <= 0){
            //              typeOneCrashFile += excelFiles(terms)
            //              break()
            //            }


            /* Simulation start */

            var breakDaysMap: Map[String, Double] = Map()
            var maximumRateMap: Map[Double, Double] = Map()
            var expectationMap: Map[Double, Double] = Map()
            var frequencyMap: Map[Double, Double] = Map()
            val breakThresholdArybuf = ArrayBuffer[Double]()

            for(j <- 0 to 4) {

              val trans = new Transaction(macdAryBuf, difAryBuf, indexCloseMap, longestDay)

              trans.transSimulation(j)
              trans.transFreqVerify()

              val threshold = trans.threshold
              //val sellIndex = trans.sellIndex
              val hasTrans = trans.testEmptyTrans()
              val transCounts = trans.transCount()

              println("count:" + transCounts)
              frequencyMap += (threshold -> transCounts)

              //Threshold has no transaction will break()
              breakable{
                if(hasTrans){
                  breakThresholdArybuf += threshold
                  typeTwoCrashFile += opIndex
                  test += 1
                  break()
                }

                val CRate = trans.calculateCum()
                val ERate = trans.calculateExp()
                val holdAndWait = trans.calculateHoldNWait()
                val STD = trans.calculateStd()

                maximumRateMap += (threshold -> trans.getMaxMinReturn(0))
                expectationMap += (threshold -> ERate)

                breakDaysMap = breakDaysMap ++ trans.breakDaysMap

                opMap += (opIndex -> ERate)
                transTime += transCounts

                println("\n Cumulative Return: " + CRate)
                println("\n Expected Return: " + ERate)
                println("\n Hold & Wait: " + holdAndWait)
                trans.resultsPrint()

                //foooooooor
                for(i <- 0 to 100) println(opIndex + "," + j)

                //database connection
                val dbConnect = new DatabaseConnection(opIndex, ERate, CRate, transCounts, STD)
                val dbNames = Array("parameterOPT", "paraWithOneTimesThreshold" ,"paraWithTwoTimesThreshold",
                  "paraWithThreeTimesThreshold", "paraWithFourTimesThreshold")

                j match {
                  case 0 => dbConnect.writeDB(dbNames(0))
                  case 1 => dbConnect.writeDB(dbNames(1))
                  case 2 => dbConnect.writeDB(dbNames(2))
                  case 3 => dbConnect.writeDB(dbNames(3))
                  case 4 => dbConnect.writeDB(dbNames(4))
                }

              }
            }

          }
        }
      }
    }

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
          if(postHis/preHis > 1.1 && postHis/postHis > 0.9){
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



    }
    opMap.foreach(println)

    typeTwoCrashFile.foreach(x => println("\n Index : " + x + " didn't have any transaction!!"))
    //typeOneCrashFile.foreach(x => println("\n File : " + x + " don't have enough data!!"))

    println("\n Max: " + opMap.maxBy(_._2))
    println("\n Length: " + opMap.size)
    println("\n Break parameter's number:" + test)

    transTime.foreach(x => println("\ntransTime: "+x))
    //sizeAry.foreach(x => println("\narySize: "+x))
    //println("\n skipDays: " + skipDays)
  }
}
