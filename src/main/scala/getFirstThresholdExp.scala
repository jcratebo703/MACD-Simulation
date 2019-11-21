import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement}

import breeze.numerics._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


object getFirstThresholdExp extends App{
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
  val terms = 44
  val index: Array[Int] = Array(30, 49, 38)

  val rows: Array[Row] = dfs(terms).collect()

  val closeArr: Array[Double] = rows.map(_.getDouble(1))
  val closeRDD = sc.parallelize(closeArr)

  val closeSum: Double = closeRDD.sum()
  val closeAvg: Double = closeRDD.mean()
  val closeNum: Double = closeRDD.count()

  println("closeNum: " + closeNum + "\ncloseAvg: " + closeAvg + "\ncloseSum: " + closeSum)

  val closeWithIndex = closeRDD.zipWithIndex()
  //println("closeWithIndex: ")
  //closeWithIndex.foreach(println)

  val indexCloseRDD = closeWithIndex.map{case (k, v) => (v, k)}

  val indexCloseMap = indexCloseRDD.collect().toMap
  //indexCloseRDD.foreach(println)

  // Close Map Done

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
  val j = 1

  opIndex = index(0).toString + "," + index(1).toString + "," + index(2).toString

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
  //println("\nDIF: " + difAryBuf)
  println("\n DIF length: " + difAryBuf.size)

  val difMap = sc.parallelize(difAryBuf).zipWithIndex.map{case (k, v) => (v, k)}.collect().toMap

  val macdAryBuf = ArrayBuffer[Double]()
  macdAryBuf ++= Ema(index(2), difMap)
  println("\nName: " + trimFiles(terms))
  //println("\nMACD: " + macdAryBuf)
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



  val trans = new Transaction(macdAryBuf, difAryBuf, indexCloseMap, longestDay)

  trans.transSimulation(j)
  trans.transFreqVerify()

  val threshold = trans.threshold
  //val sellIndex = trans.sellIndex
  val hasTrans = trans.testEmptyTrans()
  val transCounts = trans.transCount()

  println("count:" + transCounts)

  val CRate = trans.calculateCum()
  val ERate = trans.calculateExp()
  val holdAndWait = trans.calculateHoldNWait()
  val STD = trans.calculateStd()

  //opMap += (opIndex -> ERate)
  transTime += transCounts

  println("\n Cumulative Return: " + CRate)
  println("\n Expected Return: " + ERate)
  println("\n Hold & Wait: " + holdAndWait)
  trans.resultsPrint()

  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    val insertSQL = "INSERT INTO scalaTest."+ "MaxExpectationAddThreshold" + " (Name, MaxExpectation)" +
      " VALUES(?, ?)"

    val prep: PreparedStatement = connection.prepareStatement(insertSQL)

    prep.setString(1, trimFiles(terms))
    prep.setDouble(2, ERate)

    prep.execute()

    prep.close()

  } catch {
    case e: Exception => e.printStackTrace()
  }
  connection.close()


}
