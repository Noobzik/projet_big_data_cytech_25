import org.apache.spark.sql.SparkSession

object CheckSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CheckSchema")
      .master("local[*]")
      .getOrCreate()

    val inputPath = "../data/raw/yellow_tripdata_2025-01.parquet"

    println(s"Reading schema from $inputPath")
    val df = spark.read.parquet(inputPath)

    println("Schema:")
    df.printSchema()

    println("\nFirst 5 rows:")
    df.show(5)

    println("\nColumn names:")
    df.columns.foreach(println)

    spark.stop()
  }
}
