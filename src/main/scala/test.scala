import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import  scala.collection.mutable._
import  scala.math._
import org.apache.spark.sql.types._

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val dfp = spark.read.csv("/Users/caichengyun/Documents/User/CGU/Subject/三下/Financial Management/Final Project Data/1. 1216 統一.csv" )
    val withIDs = dfp.withColumn("id", monotonically_increasing_id())
    val cleanedDf = withIDs.filter(withIDs("id") >= 3)
    val df = cleanedDf.orderBy("_c0")

    df.show()
//    df.filter(df("_c1") =!= "").show()
//    val columns = df.schema.fields.filter(x => x.dataType == IntegerType || x.dataType == DoubleType)
//
//    df.select(columns.map(x => df(x.name)): _*).show()



  }

}
