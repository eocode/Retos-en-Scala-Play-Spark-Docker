import scala.io.Source
import org.apache.spark.sql.SparkSession
import scala.reflect.io.Directory
import java.io.File
import scala.util.Try

object Movies {
  def main(args: Array[String]) {
    // Session Spark
    val spark = SparkSession
      .builder()
      .appName("Movies")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Run Task
    runHeart(spark)

    spark.stop()
  }
  private def runHeart(spark: SparkSession): Unit = {

    import spark.implicits._

    // Read CSV file
    val readMoviesCSV = spark.read
      .format("csv")
      .option("sep", "	")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/home/data/u.data")

    // Remove parquet
    val directory = new Directory(new File("/home/data/movies.parquet"))
    if(directory.exists){
      directory.deleteRecursively()
      readMoviesCSV.write.parquet("/home/data/movies.parquet")
    }else{
    // Transform to Parket for columnar data
      readMoviesCSV.write.parquet("/home/data/movies.parquet")
    }

    val parquetMoviesFile = spark.read.parquet("/home/data/movies.parquet")
    parquetMoviesFile.createOrReplaceTempView("parquetMoviesFile")
    val top = spark.sql(
      "SELECT movie_id, count(movie_id) views FROM parquetMoviesFile group by movie_id order by 2 limit 10"
    )
    // Show query
    top.select($"movie_id", $"views").show()
  }
}
