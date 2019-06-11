import org.apache.spark.sql.{Row, SparkSession}

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import breeze.linalg._
import breeze.numerics._
import scala.collection.immutable.ListMap
import scala.util.Random
import scala.runtime.ScalaRunTime._

object optimizeGA {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")

    val dfInverse = df.orderBy("_c0")// the date of data must be ascending (r0=2019/01/02 r1=2019/01/01)
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

    var index: Array[Double] = Array(12, 26, 9)
    var opIndex: String = ""
    var opMap: Map[String, Double] = Map()

    val typeOneCrashFile = ArrayBuffer[String]()
    val typeTwoCrashFile = ArrayBuffer[String]()

    //EMA function

    val Ema = (index: Double, closeData: Map[Long, Double]) => {
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


    //generate GA populations

    var randMin: Double = 2
    val randMax: Double = 100
    var d = Random.nextDouble()
    var rand: Double = randMin + (randMax - randMin) * d

    d = Random.nextDouble()
    rand = randMin + (randMax - randMin) * d
    println(d)
    println(rand)

    val rowsNum: Int = 100
    val colsNum: Int = 5

    val GAPopulations = Array.ofDim[Double](rowsNum, colsNum)
    //println(a(0).size)
    //val sa = Array.fill(5)("Hi")
    for(i <- 0 until GAPopulations.size){
      d = Random.nextDouble()
      rand = randMin + (randMax - randMin) * d
      GAPopulations(i)(0) = rand
      randMin = rand
      GAPopulations(i)(1) = randMin + (randMax - randMin) * d
      d = Random.nextDouble()
      rand = randMin + (randMax - randMin) * d
      GAPopulations(i)(2) = rand
      randMin = 2
      GAPopulations(i)(3) = 0
      GAPopulations(i)(4) = 0
    }
    var GAPopulationsWithR = Array.ofDim[Double](100, 5)

    //start GA optimization

    for(i <- 0 to GAPopulations.size-1){

      opIndex = GAPopulations(i)(0).toString + "," + GAPopulations(i)(1).toString + "," + GAPopulations(i)(2).toString
      val opAry: Array[Double] = Array(GAPopulations(i)(0), GAPopulations(i)(1), GAPopulations(i)(2), 0, 0)

      index = GAPopulations(i)

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

        //            if(macdAryBuf.size <= 0){
        //              typeOneCrashFile += excelFiles(terms)
        //              break()
        //            }


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
          val close: Double = indexCloseMap.get(i+1+longestDay-1).toArray.mkString("").toDouble
          //var spend = asset * transRatio
          //var sellNumb = stockNum * transRatio

          if(preHis < 0 && postHis > 0){ //negative to positive, buy
            Buf = close
            b += 1
            buyPrice += close
            //stockNum += spend / indexCloseMap.get(i).toArray.mkString("").toDouble
            //asset -= spend
            buyIndex += i+1
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
          typeTwoCrashFile += opIndex
          opMap += (opIndex -> -100)
          opAry(3) = -100
          GAPopulationsWithR(i) = opAry
          break()
        }

        println("count:" +buyIndex.size)
        var firstBuy: Double = indexCloseMap.get(buyIndex(0)+longestDay-1).toArray.mkString("").toDouble
        var lastSell: Double = indexCloseMap.get(sellIndex(sellIndex.size-1)+longestDay-1).toArray.mkString("").toDouble
        val ERate = returnRate.sum/returnRate.size

        println("\n Sell Index: " + sellIndex + "\n Sell counts: " + sellIndex.size)
        println("\n Buy Index: " + buyIndex + "\n Buy counts: " + buyIndex.size)
        println("\n Sell Price: " + sellPrice)
        println("\n Buy Price: " + buyPrice)
        println("\n Rate of Return: " + returnRate)
        println("\n Maximum: " + returnRate.max)
        println("\n Minimum: " + returnRate.min)
        println("\n Cumulative Return Rate: " + (lastSell-firstBuy)/firstBuy)
        println("\n Expectation of Return Rate: " + ERate)

        println("\n b : " + b + "\n s : " + s)

        println("\n Simulation complete")

        for(i <- 0 to 100) println(opIndex)

        opMap += (opIndex -> ERate)
        opAry(3) = ERate
        GAPopulationsWithR(i) = opAry

      }
    }

    opMap = ListMap(opMap.toSeq.sortWith(_._2 > _._2):_*)
    opMap.foreach(println)

    GAPopulationsWithR = GAPopulationsWithR.sortWith(_(3) > _(3))

    val newGAPopulations = ArrayBuffer[Array[Double]]()
    var m = 0

    //take elite into new population
    for(i <- GAPopulationsWithR.take(5)){
      //newGAPopulations += i._1.split(",").map(_.toDouble)
      newGAPopulations += i
      i(4) = 1
      m += 1
    }

    //Tournament style
    val crossoverPopulatios = ArrayBuffer[Array[Double]]()

    do{
      var fourPopulations = Array.ofDim[Double](4, 5)
      var randInt = Random.nextInt(GAPopulationsWithR.size-1)

      for(i <- 0 to 3){//set fourPopulations
        while(GAPopulationsWithR(randInt)(4) == 1){
          randInt = Random.nextInt(GAPopulationsWithR.size-1)
          //println(randInt)
        }
        fourPopulations(i) = GAPopulationsWithR(randInt)
        GAPopulationsWithR(randInt)(4) = 1
      }

      fourPopulations = fourPopulations.sortWith(_(3) > _(3))
      //println(fourPopulations.deep.mkString("\n"))
      for(i <- 0 to GAPopulationsWithR.size-1){//set flag = 0
        //val x = fourPopulations(0)
        if(GAPopulationsWithR(i)(1) == fourPopulations(1)(1))GAPopulationsWithR(i)(4) = 0
        if(GAPopulationsWithR(i)(1) == fourPopulations(2)(1))GAPopulationsWithR(i)(4) = 0
        if(GAPopulationsWithR(i)(1) == fourPopulations(3)(1))GAPopulationsWithR(i)(4) = 0
      }
//      for(i <- 0 to GAPopulationsWithR.size-1){
//        if(GAPopulationsWithR(i)(4) == 1){
//          println(GAPopulationsWithR(i).mkString("[ ", ", ", " ]"))
//        }
//      }

      crossoverPopulatios += fourPopulations(0)
      //println(fourPopulations(0).mkString("[ ", ", ", " ]"))

    }while(crossoverPopulatios.size < 76)

    //two-point crossover
    do{
      val twoPopulations = Array.ofDim[Double](2, 5)
      var randInt = Random.nextInt(crossoverPopulatios.size-1)

      for(i <- 0 to 1){//set fourPopulations
        while(crossoverPopulatios(randInt)(4) == 0){
          randInt = Random.nextInt(crossoverPopulatios.size-1)
          //println(randInt)
        }
        twoPopulations(i) = crossoverPopulatios(randInt)
        crossoverPopulatios(randInt)(4) = 0
      }
      val r0c0: Double = twoPopulations(0)(0)
      val r0c1: Double = twoPopulations(0)(1)
      val r0c2: Double = twoPopulations(0)(2)

      twoPopulations(0)(1) = twoPopulations(1)(1)
      twoPopulations(1)(1) = r0c1
      println(twoPopulations.deep.mkString("\n"))

      twoPopulations(0)(0) = twoPopulations(1)(0)
      twoPopulations(0)(2) = twoPopulations(1)(2)
      twoPopulations(1)(0) = r0c0
      twoPopulations(1)(2) = r0c2

      println(twoPopulations.deep.mkString("\n"))
      for(i <- 0 to crossoverPopulatios.size-1){//set flag = 0
        //val x = fourPopulations(0)
        if(crossoverPopulatios(i)(1) == twoPopulations(0)(1))crossoverPopulatios(i)(4) = 1
        if(crossoverPopulatios(i)(1) == twoPopulations(1)(1))crossoverPopulatios(i)(4) = 1
      }
      newGAPopulations += twoPopulations(0)
      newGAPopulations += twoPopulations(1)
      //println(fourPopulations(0).mkString("[ ", ", ", " ]"))

    }while(newGAPopulations.size < 81)

    typeTwoCrashFile.foreach(x => println("\n Index : " + x + " don't have any transaction!!"))
    //typeOneCrashFile.foreach(x => println("\n File : " + x + " don't have enough data!!"))

    println("\n Max: " + opMap.maxBy(_._2))
    println("\n Length: " + opMap.size)

    //println(GAPopulationsWithR.deep.mkString("\n"))
    crossoverPopulatios.foreach(x => println(x.mkString("[ ", ", ", " ]")))
    println(crossoverPopulatios.size)

    newGAPopulations.foreach(x => println(x.mkString("[ ", ", ", " ]")))
    println(newGAPopulations.size)

  }
}
