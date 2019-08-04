import org.apache.spark.sql._

import scala.collection.mutable._
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

val longestDays: Array[Int] = Array(75, 76, 75)

val skipDays: Int = (3.45*(longestDays(1)+1)).ceil.toInt+(3.45*(longestDays(2)+1)).ceil.toInt-2//OMG
println("skipDays: " + skipDays)

println("test: " + (longestDays(1)+1)*3.45.ceil)

var test0 = ArrayBuffer[Int]()
for(i <- 1 to 5) test0 += i
//test0.transform(_+1)

test0.foreach(x => println("\n"+ x))

var cumulativeRate: Double = 1
test0.foreach(x => cumulativeRate *= x)

println(cumulativeRate)

