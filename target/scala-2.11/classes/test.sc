import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


val singleFileExpMap = Map("Neil" -> 97.123456, "Buzz" -> 97.123456, "Michael" -> 95.006489)
val maxExpectation: Double = singleFileExpMap.valuesIterator.max
//val maxCumulation: Double = singleFileCumMap.valuesIterator.max
val maxExpectationKey: String = singleFileExpMap.find(_._2 == maxExpectation).map(_._1).mkString

val MaxExpSTD: Double = singleFileExpMap.filter(_._1 == maxExpectationKey).values.mkString.toDouble

//println(5)
//
//for(x <- 0 to 10) {
//  //breakable{
//  for (y <- 0 to 10) {
//    breakable {
//      for (z <- 0 to 10) {
//        if (z > 5) {
//          break()
//        }
//        println(x.toString + " " + y.toString + " " + z.toString)
//      }
//    }
//  }
////}
//}