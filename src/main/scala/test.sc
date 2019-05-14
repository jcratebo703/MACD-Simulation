import org.apache.spark.sql.SparkSession
//import breeze.linalg._
//import breeze.plot._


val spark = SparkSession.builder()
  .appName("GitHub push counter")
  .master("local[*]")
  .getOrCreate()

var df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")
var colDf = df.collect()


val price = Array(for(i <- 0 to colDf.length-1) colDf(i)(4))
println(price)

var sum: Int = 0
println(for(i <- 0 to price.length) sum += price(i).toString().toInt)

for(i <- 0 to indexCloseRDD.count().toInt){
  indexCloseRDD.lookup(i).foreach(println)
}

//println()