import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


val singleFileExpMap = Map("Neil" -> 97.123456, "Buzz" -> 71.78945, "Michael" -> 95.006489)
val maxExpectation: Double = singleFileExpMap.valuesIterator.max
//val maxCumulation: Double = singleFileCumMap.valuesIterator.max
val maxExpectationKey: String = singleFileExpMap.filter(_._2 == maxExpectation).keys.mkString

val MaxExpSTD: Double = singleFileExpMap.filter(_._1 == maxExpectationKey).values.mkString.toDouble