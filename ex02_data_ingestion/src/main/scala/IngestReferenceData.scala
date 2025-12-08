import org.apache.spark.sql.{SparkSession, SaveMode}
import java.util.Properties

object IngestReferenceData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("IngestReferenceData")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Path to the reference data
    val inputPath = "../data/raw/taxi_zone_lookup.csv"

    println(s"Reading reference data from $inputPath")

    try {
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath)

      println("Schema:")
      df.printSchema()
      df.show(5)

      // Postgres Connection Properties
      val jdbcUrl = "jdbc:postgresql://localhost:5433/warehouse"
      val connectionProperties = new Properties()
      connectionProperties.put("user", "postgres")
      connectionProperties.put("password", "password")
      connectionProperties.put("driver", "org.postgresql.Driver")

      println(s"Writing to Postgres table 'dim_location' at $jdbcUrl")

      // Rename columns to match Postgres schema if necessary
      // Postgres schema: location_id, borough, zone, service_zone
      // CSV schema: LocationID, Borough, Zone, service_zone
      val postgresDf = df.select(
        $"LocationID".as("location_id"),
        $"Borough".as("borough"),
        $"Zone".as("zone"),
        $"service_zone"
      )

      postgresDf.write
        .mode(
          SaveMode.Append
        ) // Use Append to add to existing data (or Overwrite if you want to reset)
        .jdbc(jdbcUrl, "dim_location", connectionProperties)

      println("Reference data ingestion completed successfully.")

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
