import org.apache.spark.sql._
import  scala.collection.mutable._
import  scala.math._

val spark = SparkSession.builder()
  .appName("GitHub push counter")
  .master("local[*]")
  .getOrCreate()

val sc = spark.sparkContext
val df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")


val arybuf1 = ArrayBuffer[Double]()
val arybuf2 = ArrayBuffer[Double]()

arybuf1 += 1
arybuf1 += 10

arybuf2 += 2
arybuf2 += 20

arybuf2.foreach(x => x-1)

for(i <- 10 to 1 by -1)println(i)

df.show(10)

