import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.File
import org.apache.spark.rdd._
import scala.math._
import scala.util.Random

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    //      val df = spark.read.csv("/Users/caichengyun/Documents/codingBuf/tejdb_20190129160802 copy.csv")
    //      val dfInverse = df.orderBy("_c0")// the date of data must be ascending (r0=2019/01/02 r1=2019/01/01)
    val path: String = "/Users/caichengyun/Documents/User/CGU/Subject/FYP/Taiwan50Index/"

    val Schema = StructType(Array(
      StructField("Date", StringType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("Open", DoubleType, nullable = false),
      StructField("Highest", DoubleType, nullable = false),
      StructField("Lowest", DoubleType, nullable = false),
      StructField("id", IntegerType, nullable = false)))

    def readExcel(file: String): DataFrame = spark.read
      .format("com.crealytics.spark.excel")
      .schema(Schema) // Optional, default: Either inferred schema, or all columns are Strings
      //      .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      .option("useHeader", "false") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "false") // Optional, default: false
      .option("timestampFormat", "yyyy/mm/dd HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      //      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      //      .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
      //      .option("location", path)
      .load(file)

    val dir = new File(path)
    val excelFiles = dir.listFiles.sorted.map(f => f.toString)  // Array[String]
    excelFiles.foreach(println)

    val dfs = excelFiles.map(f => readExcel(f).withColumn("id", monotonically_increasing_id())
      .filter(col("id") >= 2).orderBy("Date"))  // Array[DataFrame]  .orderBy("Date")
    //val ppdf = dfs.reduce(_.union(_))

    val trimFiles = excelFiles.map(x =>  x.replaceAll("^.{67}", "")
      .replaceAll("[a-z]|[0-9]|/|\\.| ", ""))

//    dfs.foreach(x => x.show())
//    trimFiles.foreach(println)

    dfs(0).show()
    println(trimFiles(0))

    for(terms <- dfs.indices){

    }

  }
}
