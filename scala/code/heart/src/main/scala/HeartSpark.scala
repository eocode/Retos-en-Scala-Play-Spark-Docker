import scala.io.Source
import org.apache.spark.sql.SparkSession
import scala.reflect.io.Directory
import java.io.File
import scala.util.Try

object HeartSpark {
  def main(args: Array[String]) {
    // Session Spark
    val spark = SparkSession
      .builder()
      .appName("Heart")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Run Task
    runHeart(spark)

    spark.stop()
  }
  private def runHeart(spark: SparkSession): Unit = {

    import spark.implicits._

    // Read CSV file
    val readHeartCSV = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/home/data/heart.csv")

    // Remove parquet
    val directory = new Directory(new File("/home/data/heart.parquet"))
    if(directory.exists){
      directory.deleteRecursively()
      readHeartCSV.write.parquet("/home/data/heart.parquet")
    }else{
    // Transform to Parket for columnar data
      readHeartCSV.write.parquet("/home/data/heart.parquet")
    }

    val parquetHeartFile = spark.read.parquet("/home/data/heart.parquet")
    parquetHeartFile.createOrReplaceTempView("parquetHeartFile")
    val age = spark.sql(
      "SELECT AVG(chol) as avg FROM parquetHeartFile where age BETWEEN 40 AND 50"
    )
    // Show query
    age.select($"avg").show()
  }
}
