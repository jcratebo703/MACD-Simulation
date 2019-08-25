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
            returnRate += (close - Buf) / Buf
            sellPrice += close

            hold = 0
            Buf = 0
          }
        }
      }
    }
  }

  def trasFreqVerify(x: Unit): Unit ={
    if (sellIndex.size != buyIndex.size || sellIndex.size != returnRate.size) {
      buyIndex.remove(buyIndex.size - 1)
      buyPrice.remove(buyPrice.size - 1)
      //println("\n error occur!!!!!")
    }
  }

  def resultsPrint(x: Unit): Unit ={
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

}
