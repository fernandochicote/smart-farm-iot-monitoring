ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "smart-farm-iot-monitoring"
  )

// Importaciones necesarias para trabajar con Spark y Kafka
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"            % Versions.spark,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark,
  // Delta Lake
  "io.delta"        %% "delta-spark"           % Versions.delta,
  // ScalaTest
  "org.scalatest"   %% "scalatest"             % Versions.scalaTest % Test,
  // Spark Fast Tests
  "com.github.mrpowers" %% "spark-fast-tests"  % Versions.sparkFastTests % Test
)
