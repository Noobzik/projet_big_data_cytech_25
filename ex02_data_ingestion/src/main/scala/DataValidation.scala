import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import io.minio.{
  MinioClient,
  UploadObjectArgs,
  BucketExistsArgs,
  MakeBucketArgs
}
import java.io.File
import java.util.Properties

object DataValidation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataIngestion")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Lecture des données parquet
    val inputPath = "../data/raw/yellow_tripdata_2025-01.parquet"
    val localOutputPath = "temp_validated_data"

    println(s"Lecture depuis $inputPath")

    try {
      val df = spark.read.parquet(inputPath)

      println(s"Total records: ${df.count()}")

      // Filtrage des données invalides
      val validatedDf = df.filter(
        col("passenger_count") > 0 &&
          col("trip_distance") >= 0 &&
          col("total_amount") >= 0 &&
          col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")
      )

      val validCount = validatedDf.count()
      println(s"Records valides: $validCount")

      // --- Etape 1: Stockage Minio ---
      println("--- Branch 1: Stockage Minio ---")
      // Sauvegarde locale avant upload

      // Clean up previous run if exists
      val outputDir = new File(localOutputPath)
      if (outputDir.exists()) {
        deleteRecursively(outputDir)
      }

      validatedDf.write
        .mode("overwrite")
        .parquet(localOutputPath)

      // Upload vers le bucket
      uploadToMinio(
        localOutputPath,
        "warehouse",
        "validated/yellow_tripdata_2025-01"
      )
      println("Upload Minio terminé.")

      // --- Etape 2: Ingestion Postgres ---
      println("--- Branch 2: Ingestion Postgres ---")

      // Mapping vers le schéma fact_trips
      val postgresDf = validatedDf.select(
        col("VendorID").as("vendor_id"),
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("RatecodeID").as("rate_code_id"),
        col("store_and_fwd_flag"),
        col("PULocationID").as("pulocation_id"),
        col("DOLocationID").as("dolocation_id"),
        col("payment_type").as("payment_type_id"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("improvement_surcharge"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("Airport_fee").as("airport_fee")
      )

      // Configuration JDBC via Env Vars
      val dbHost = sys.env.getOrElse("DB_HOST", "localhost")
      val dbPort = sys.env.getOrElse("DB_PORT", "5433")
      val dbName = sys.env.getOrElse("DB_NAME", "warehouse")
      val dbUser = sys.env.getOrElse("DB_USER", "postgres")
      val dbPass = sys.env.getOrElse("DB_PASS", "password")

      val jdbcUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
      val connectionProperties = new Properties()
      connectionProperties.put("user", dbUser)
      connectionProperties.put("password", dbPass)
      connectionProperties.put("driver", "org.postgresql.Driver")

      // Ecriture en base
      println(s"Ecriture dans Postgres ($jdbcUrl)...")
      println(s"Postgres DataFrame count: ${postgresDf.count()}")

      postgresDf.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "fact_trips", connectionProperties)

      println("Pipeline terminé avec succès.")

    } catch {
      case e: Exception =>
        println(s"Erreur: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  def uploadToMinio(
      localPath: String,
      bucketName: String,
      remotePath: String
  ): Unit = {
    // Configuration Minio
    val minioEndpoint =
      sys.env.getOrElse("MINIO_ENDPOINT", "http://localhost:9000")
    val minioAccessKey = sys.env.getOrElse("MINIO_ACCESS_KEY", "minio")
    val minioSecretKey = sys.env.getOrElse("MINIO_SECRET_KEY", "minio123")

    val minioClient = MinioClient
      .builder()
      .endpoint(minioEndpoint)
      .credentials(minioAccessKey, minioSecretKey)
      .build()

    // Création du bucket si nécessaire
    val found = minioClient.bucketExists(
      BucketExistsArgs.builder().bucket(bucketName).build()
    )
    if (!found) {
      minioClient.makeBucket(
        MakeBucketArgs.builder().bucket(bucketName).build()
      )
    }

    // Upload files
    val dir = new File(localPath)
    if (dir.exists() && dir.isDirectory) {
      val files = dir.listFiles()
      if (files != null) {
        files.foreach { file =>
          if (file.isFile && !file.getName.startsWith(".")) { // Skip hidden files like .crc
            val objectName = s"$remotePath/${file.getName}"
            println(s"Uploading ${file.getName} to $objectName")

            minioClient.uploadObject(
              UploadObjectArgs
                .builder()
                .bucket(bucketName)
                .`object`(objectName)
                .filename(file.getAbsolutePath)
                .build()
            )
          }
        }
      }
    }
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
}
