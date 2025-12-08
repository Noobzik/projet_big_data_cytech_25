name := "ex02_data_ingestion"

version := "0.1"

scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.minio" % "minio" % "8.5.7",
  "org.postgresql" % "postgresql" % "42.6.0"
)

// Add JVM options to fix illegal reflective access warnings
run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
run / fork := true
