import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.File
import org.apache.spark.rdd._
import scala.math._

object test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

//    val sc = spark.sparkContext
//    val dfp = spark.read.csv("/Users/caichengyun/Documents/User/CGU/Subject/畢專/CSV/tejdb_20190129160802.csv" )
//    val withIDs = dfp.withColumn("id", monotonically_increasing_id())
//    val cleanedDf = withIDs.filter(withIDs("id") >= 3)
//    val df = cleanedDf.orderBy("_c0")
//
//    df.show()
//    df.filter(df("_c1") =!= "").show()
//    val columns = df.schema.fields.filter(x => x.dataType == IntegerType || x.dataType == DoubleType)
//
//    df.select(columns.map(x => df(x.name)): _*).show()



    //val sc = spark.sparkContext
    val path: String = "/Users/caichengyun/Documents/User/CGU/Subject/三下/Financial Management/Final Project Data"

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

    val dfs = excelFiles.map(f => readExcel(f).withColumn("id", monotonically_increasing_id()).filter(col("id") >= 2).orderBy("Date"))  // Array[DataFrame]  .orderBy("Date")
    val ppdf = dfs.reduce(_.union(_))

    dfs(1).show()
    dfs(2).show()
    dfs(3).show()
    dfs(4).show()



    ppdf.printSchema()
    ppdf.show(800)

   // println(ppdf.count())



  }

}
