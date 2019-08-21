import org.apache.spark.sql.{Row, SparkSession}
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.unix_timestamp
import breeze.linalg._
import breeze.numerics._

import java.sql.{Connection, DriverManager, PreparedStatement}

object MACDParameterOptimization extends App{
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
  var opIndex: String = ""
  var opMap: Map[String, Double] = Map()

  //val typeOneCrashFile = ArrayBuffer[String]()
  val typeTwoCrashFile = ArrayBuffer[String]()

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

  var test: Int = 0
  val longestDays: Array[Int] = Array(50, 51, 50)
  val longest3rdNDay = (3.45*(longestDays(2)+1)).ceil.toInt

  val skipDays: Int = (3.45*(longestDays(1)+1)).ceil.toInt+ longest3rdNDay -2//OMG
  println("skipDays: " + skipDays)

  val sizeAry = ArrayBuffer[Int]()
  val transTime = ArrayBuffer[Int]()

  for(x <- longestDays(0) to 1 by -1){
    for(y <- longestDays(1) to x + 1 by -1){
      for(z <- longestDays(2) to 1 by -1){

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

          var Buf: Double = -1
          var hold: Int = 0
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
            val close: Double = indexCloseMap.get(i+1+longestDay-1).toArray.mkString("").toDouble
            //var spend = asset * transRatio
            //var sellNumb = stockNum * transRatio

            if(preHis < 0 && postHis > 0){ //negative to positive, buy
              hold = 1
              Buf = close
              b += 1
              buyPrice += close
              //stockNum += spend / indexCloseMap.get(i).toArray.mkString("").toDouble
              //asset -= spend
              buyIndex += i+1
            }
            else if(preHis > 0 && postHis < 0){ //sell
              //asset += indexCloseMap.get(i).toArray.mkString("").toDouble * sellNumb
              //stockNum -= sellNumb
              if(hold == 1){
                s += 1
                sellIndex += i+1
                priceDif += Buf - close
                returnRate += (close-Buf)/Buf
                sellPrice += close

                hold = 0
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
            typeTwoCrashFile += opIndex
            test += 1
            break()
          }

          println("count:" +buyIndex.size)
          val firstBuy: Double = indexCloseMap.get(buyIndex(0)+longestDay-1).toArray.mkString("").toDouble
          val lastSell: Double = indexCloseMap.get(sellIndex(sellIndex.size-1)+longestDay-1).toArray.mkString("").toDouble
          val ERate = returnRate.sum/returnRate.size

          val ERateAddOne = returnRate
          ERateAddOne.transform(_+1)
          var cumulativeRate: Double = 1
          ERateAddOne.foreach(x => cumulativeRate *= x)
          cumulativeRate -= 1

          println("\n Sell Index: " + sellIndex + "\n Sell counts: " + sellIndex.size)
          println("\n Buy Index: " + buyIndex + "\n Buy counts: " + buyIndex.size)
          println("\n Sell Price: " + sellPrice)
          println("\n Buy Price: " + buyPrice)
          println("\n Rate of Return: " + returnRate)
          println("\n Maximum: " + returnRate.max)
          println("\n Minimum: " + returnRate.min)
          println("\n Cumulative Return Rate: " + (lastSell-firstBuy)/firstBuy)
          println("\n The Real Cumulative Return Rate: " + cumulativeRate)
          println("\n Expectation of Return Rate: " + ERate)

          println("\n b : " + b + "\n s : " + s)

          println("\n Simulation complete")

          for(i <- 0 to 100) println(opIndex)

          opMap += (opIndex -> ERate)
          transTime += buyIndex.size

          try {
            Class.forName(driver)
            connection = DriverManager.getConnection(url, username, password)
            //val statement = connection.createStatement
            //    val rs = statement.executeQuery("SELECT Name, TranFrequency FROM scalaTest.cop")
            //    while (rs.next) {
            //      val name = rs.getString("Name")
            //      val freq = rs.getInt("TranFrequency")
            //      println("name = %s, freq = %d".format(name,freq))
            //    }

            val insertSQL = "INSERT INTO scalaTest.parameterOPT (parameters, ERate, CRate, Frequency)" +
              " VALUES(?, ?, ?, ?)"

            val prep: PreparedStatement = connection.prepareStatement(insertSQL)

            prep.setString(1, opIndex)
            prep.setDouble(2, ERate)
            prep.setDouble(3, cumulativeRate)
            prep.setInt(4, buyIndex.size)
            prep.execute()

            prep.close()

          } catch {
            case e: Exception => e.printStackTrace
          }
          connection.close

        }
      }
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
