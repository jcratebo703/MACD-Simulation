import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class Transaction(val macdAryBuf: ArrayBuffer[Double], val difAryBuf: ArrayBuffer[Double], val indexCloseMap: Map[Long, Double], val longestDay: Int){
  val range: Double = 0.05
  var threshold: Double = 0
  var hold: Int = 0
  var Buf: Double = 0
  val sellIndex = ArrayBuffer[Int]()
  val buyIndex = ArrayBuffer[Int]()
  val sellPrice = ArrayBuffer[Double]()
  val buyPrice = ArrayBuffer[Double]()
  val priceDif = ArrayBuffer[Double]()
  val returnRate = ArrayBuffer[Double]()
  var b, s: Int = 0
  var breakDaysMap: Map[String, Double] = Map()

  def transSimul(thrTimes: Int): Unit = {
    this.threshold = thrTimes * range

    for (i <- 0 to macdAryBuf.size - 2) {
      breakable{
        val preHis = difAryBuf(i) - macdAryBuf(i)
        val postHis = difAryBuf(i + 1) - macdAryBuf(i + 1)
        //println("\npostHis: " + postHis)
        val close: Double = indexCloseMap.get(i + 1 + longestDay - 1).toArray.mkString("").toDouble

        if (preHis < 0 && postHis > 0) { //negative to positive, buy
          if(threshold == 0 || postHis >= threshold) {
            //println("TRUE")
            hold = 1
            Buf = close
            b += 1
            buyPrice += close
            //stockNum += spend / indexCloseMap.get(i).toArray.mkString("").toDouble
            //asset -= spend
            buyIndex += i + 1
          }
          else{
            //println("False")
            breakDaysMap += (threshold.toString + "," + (i + 1).toString -> preHis)
            break()
          }
        }
        else if (preHis > 0 && postHis < 0) {
          //asset += indexCloseMap.get(i).toArray.mkString("").toDouble * sellNumb
          //stockNum -= sellNumb
          if (hold == 1) {
            s += 1
            sellIndex += i + 1
            priceDif += Buf - close
            val eR = (close - Buf) / Buf
            returnRate += eR
            sellPrice += close

            hold = 0
            Buf = 0
          }
        }
      }
    }
  }

  def transFreqVerify(x: Unit): Unit ={
    if (sellIndex.size != buyIndex.size || sellIndex.size != returnRate.size) {
      buyIndex.remove(buyIndex.size - 1)
      buyPrice.remove(buyPrice.size - 1)
      println("\n last transaction was buy")
    }
  }

  def resultsPrint(x: Unit): Unit ={
    println("\n")
    println("\n Sell Index: " + sellIndex + "\n Sell counts: " + sellIndex.size)
    println("\n Buy Index: " + buyIndex + "\n Buy counts: " + buyIndex.size)
    println("\n Sell Price: " + sellPrice)
    println("\n Buy Price: " + buyPrice)
    println("\n Rate of Return: " + returnRate)
    println("\n Maximum: " + returnRate.max)
    println("\n Minimum: " + returnRate.min)
    println("\n b : " + b + "\n s : " + s)

    println("\nSimulation complete\n")
  }

  def calculateCum(x: Unit): Double ={
    val ERateAddOne = returnRate.clone() // call by address warning !!
    ERateAddOne.transform(_+1)
    var cumulativeRate: Double = 1
    ERateAddOne.foreach(x => cumulativeRate *= x)
    cumulativeRate -= 1
    cumulativeRate
  }

  def calculateExp(x: Unit): Double ={
    val ERate = returnRate.sum / returnRate.size
    ERate
  }

  def calculateHoldNWait(x: Unit):Double ={
    val firstBuy: Double = indexCloseMap.get(buyIndex(0) + longestDay - 1).toArray.mkString("").toDouble
    val lastSell: Double = indexCloseMap.get(sellIndex(sellIndex.size - 1) + longestDay - 1).toArray.mkString("").toDouble
    (lastSell - firstBuy) / firstBuy
  }

}
