import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement}

import breeze.numerics._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.immutable.ListMap
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
  val path: String = "/Users/caichengyun/Documents/User/CGU/Subject/FYP/Taiwan50Index/"

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

  val trimFiles = excelFiles.map(x =>  x.replaceAll("^.{67}", "")
    .replaceAll("[a-z]|[0-9]|/|\\.| ", ""))

  //dfs & Names Done

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

    for(j <- 0 until closeData.size){

      if(j - (Nday-1) >= 0) {

        if(j == Nday-1){
          for (i <- 0 until Nday) {
            // emaAryBuffer += indexCloseRDD.lookup(i).toArray.mkString("").toDouble
            emaAryBuffer += closeData.get(i).toArray.mkString("").toDouble
            //var buf: Double = 0
          }

          for(k <- emaAryBuffer.indices){
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
  for(terms <- dfs.indices){

    val rows: Array[Row] = dfs(terms).collect()

    val closeArr: Array[Double] = rows.map(_.getDouble(1))
    val closeRDD = sc.parallelize(closeArr)

    val closeSum: Double = closeRDD.sum()
    val closeAvg: Double = closeRDD.mean()
    val closeNum: Double = closeRDD.count()

    println("closeNum: " + closeNum + "\ncloseAvg: " + closeAvg + "\ncloseSum: " + closeSum)

    val closeWithIndex = closeRDD.zipWithIndex()
    println("closeWithIndex: ")
    closeWithIndex.foreach(println)

    val indexCloseRDD = closeWithIndex.map{case (k, v) => (v, k)}

    val indexCloseMap = indexCloseRDD.collect().toMap
    //indexCloseRDD.foreach(println)

    // Close Map Done

    val index: Array[Int] = Array(12, 26, 9)
    var opIndex: String = ""
    //var opMap: Map[String, Double] = Map()

    //Ema(index(0)).foreach(println)

    var test: Int = 0
    val longestDays: Array[Int] = Array(30, 51, 50)
    val longest3rdNDay = (3.45*(longestDays(2)+1)).ceil.toInt

    val skipDays: Int = (3.45*(longestDays(1)+1)).ceil.toInt+ longest3rdNDay -2//OMG
    println("skipDays: " + skipDays)

    val sizeAry = ArrayBuffer[Int]()
    val transTime = ArrayBuffer[Int]()

    //multiple files analysis

    //threshold for loop
    for(j <- 0 to 4) {

      var originalExp: Double = 0
      var originalCum: Double = 0
      var originalSTD: Double = 0
      var singleFileExpMap: Map[String, Double] = Map()
      var singleFileCumMap: Map[String, Double] = Map()
      var singleFileSTDMap: Map[String, Double] = Map()

      //Start para's OPT
      for(x <- longestDays(0) to 5 by -1){
        for(y <- longestDays(1) to x + 5 by -1){
          for(z <- longestDays(2) to 2 by -1){
            //Threshold has no transaction will break()
            breakable{

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
              for(i <- longestDay - 1 until emaAryBuf1.size){
                difAryBuf += emaAryBuf1(i) - emaAryBuf2(i)
              }
              println("\nDIF: " + difAryBuf)
              println("\n DIF length: " + difAryBuf.size)

              val difMap = sc.parallelize(difAryBuf).zipWithIndex.map{case (k, v) => (v, k)}.collect().toMap

              val macdAryBuf = ArrayBuffer[Double]()
              macdAryBuf ++= Ema(index(2), difMap)
              println("\nName: " + trimFiles(terms))
              println("\nMACD: " + macdAryBuf)
              println( "\n MACD length: " + macdAryBuf.size)


              //equal all parameter's trans days
              //if(difAryBuf.size - longest3rdNDay + 1 > shortestTransDays){}
              val daysWillBeTrimmed = difAryBuf.size - shortestTransDays
              macdAryBuf.remove(0, daysWillBeTrimmed)
              difAryBuf.remove(0, daysWillBeTrimmed)

              sizeAry += macdAryBuf.size

              if(macdAryBuf.size <= 0){
                typeOneCrashFile += trimFiles(terms)+": "+opIndex
                break()
              }

              /* Simulation start */

              var breakDaysMap: Map[String, Double] = Map()
              var maximumRateMap: Map[Double, Double] = Map()
              var expectationMap: Map[Double, Double] = Map()
              var frequencyMap: Map[Double, Double] = Map()
              val breakThresholdAryBuf = ArrayBuffer[Double]()

              val trans = new Transaction(macdAryBuf, difAryBuf, indexCloseMap, longestDay)

              trans.transSimulation(j)
              trans.transFreqVerify()

              val threshold = trans.threshold
              //val sellIndex = trans.sellIndex
              val hasTrans = trans.testEmptyTrans()
              val transCounts = trans.transCount()

              if(hasTrans){
                breakThresholdAryBuf += threshold
                typeTwoCrashFile += trimFiles(terms)+ ": " +opIndex
                test += 1
                break()
              }

              println("count:" + transCounts)
              frequencyMap += (threshold -> transCounts)

              val CRate = trans.calculateCum()
              val ERate = trans.calculateExp()
              val holdAndWait = trans.calculateHoldNWait()
              val STD = trans.calculateStd()

              //multiple Files analysis

              singleFileExpMap += (opIndex -> ERate)
              singleFileCumMap += (opIndex -> CRate)
              singleFileSTDMap += (opIndex -> STD)

              if(x == 12 && y == 26 && z == 9){

                originalExp = ERate
                originalCum = CRate
                originalSTD = STD
              }

              maximumRateMap += (threshold -> trans.getMaxMinReturn(0))
              expectationMap += (threshold -> ERate)

              breakDaysMap = breakDaysMap ++ trans.breakDaysMap

              //opMap += (opIndex -> ERate)
              transTime += transCounts

              println("\n Cumulative Return: " + CRate)
              println("\n Expected Return: " + ERate)
              println("\n Hold & Wait: " + holdAndWait)
              trans.resultsPrint()

            }
            //foooooooor
            for(_ <- 0 to 100) println(opIndex + "," + j)
          }
        }
      }

      val maxExpectation: Double = singleFileExpMap.valuesIterator.max
      val maxCumulation: Double = singleFileCumMap.valuesIterator.max
      val maxExpectationKey: String = singleFileExpMap.filter(_._2 == maxExpectation).keys.mkString
      val maxCumulationKey: String = singleFileCumMap.filter(_._2 == maxCumulation).keys.mkString
      val MaxExpSTD: Double = singleFileSTDMap.filter(_._1 == maxExpectationKey).values.mkString.toDouble

      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)

        val insertSQL = "INSERT INTO scalaTest."+ "finalFYP" + " (Name, Threshold, MaxExpParameter, MaxExpectation" +
          ", OriginalExpectation, MaxExpSTD, MaxCumParameter, MaxCumulation, OriginalCumulation, OriginalSTD)" +
          " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        val prep: PreparedStatement = connection.prepareStatement(insertSQL)

        prep.setString(1, trimFiles(terms))
        prep.setString(2, (j*0.05).toString)
        prep.setString(3, maxExpectationKey)
        prep.setDouble(4, maxExpectation)
        prep.setDouble(5, originalExp)
        prep.setDouble(6, MaxExpSTD)
        prep.setString(7, maxCumulationKey)
        prep.setDouble(8, maxCumulation)
        prep.setDouble(9, originalCum)
        prep.setDouble(10, originalSTD)
        prep.execute()

        prep.close()

      } catch {
        case e: Exception => e.printStackTrace()
      }
      connection.close()

      singleFileExpMap = ListMap(singleFileExpMap.toSeq.sortWith(_._2 > _._2):_*)
      println("\nMaximum Rates: ")
      singleFileExpMap.foreach(println)

      println("pulse")
    }

    //opMap.foreach(println)

    typeTwoCrashFile.foreach(x => println("\n File : " + x + " don't have any transaction!!"))
    typeOneCrashFile.foreach(x => println("\n File : " + x + " don't have enough data!!"))
    typeThreeCrashFile.foreach(x => println("\n File : " + x + "have certain close price error!!"))

    //println("\n Max: " + opMap.maxBy(_._2))
    //println("\n Length: " + opMap.size)
    println("\n Break parameter's number:" + test)

    transTime.foreach(x => println("\ntransTime: "+x))
    //sizeAry.foreach(x => println("\narySize: "+x))
    //println("\n skipDays: " + skipDays)
  }
}
