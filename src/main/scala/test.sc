import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer


for(i <- 0 to 3){
  val emaAryBuffer = ArrayBuffer[Double]()

  emaAryBuffer += i

  emaAryBuffer.foreach(println)

  println("\n9999999999999999999")
}