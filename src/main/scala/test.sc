import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


for(i <- 0 to 10){
  breakable{
    if(i == 4)break()
    else println(i)
  }

  println("\n9999999999999999999")
}