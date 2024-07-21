package main

// Importaciones de Spark y configuración
import config.Config
import config.Config._
import main.DataValidations.validarDatosSensorTemperatureHumidity
import main.Main.TemperatureHumidityData
import main.SensorIdEnum._
import org.apache.spark.sql.functions.{col, explode, broadcast, window, avg}
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.util.LongAccumulator

// Importaciones estándar de Java y Scala
import java.sql.Timestamp
import scala.util.Try

object DataValidations {

  // Función para convertir un String a un SensorId
  def sensorIdEnumFromString(value: String): Option[SensorId] = {
    value match {
      case "sensor1" => Some(Sensor1)
      case "sensor2" => Some(Sensor2)
      case "sensor3" => Some(Sensor3)
      case "sensor4" => Some(Sensor4)
      case "sensor5" => Some(Sensor5)
      case "sensor6" => Some(Sensor6)
      case "sensor7" => Some(Sensor7)
      case "sensor8" => Some(Sensor8)
      case "sensor9" => Some(Sensor9)
      case _ => Some(Unknown)
    }
  }

  // Validar datos de temperatura y humedad
  def validarDatosSensorTemperatureHumidity(value: String, timestamp: Timestamp, errorCounter: LongAccumulator): Option[TemperatureHumidityData] = {
    val parts = value.split(",")
    val sensorIdInt = sensorIdEnumFromString(parts(0)).getOrElse(Unknown)

    // Contar el error solo una vez por cada registro
    if (sensorIdInt == Unknown) {
      errorCounter.add(1)
      None
    } else if (parts.length == 4) {  // Asegúrate de que el número de partes coincida con el formato del mensaje
      for {
        temperature <- toDouble(parts(1))
        humidity <- toDouble(parts(2))
      } yield TemperatureHumidityData(sensorIdInt, temperature, humidity, timestamp)
    } else {
      None
    }
  }

  // Función auxiliar para convertir String a Double
  private def toDouble(value: String): Option[Double] = Try(value.toDouble).toOption

  // Función auxiliar para convertir String a Timestamp
  private def toTimestamp(value: String): Option[Timestamp] = Try(Timestamp.valueOf(value)).toOption

}

object Main extends App {

  // Clase para representar los datos de un sensor de temperatura y humedad
  case class TemperatureHumidityData(sensorId: SensorId, temperature: Double, humidity: Double, timestamp: Timestamp)

  // Clase para representar todos los datos de los sensores
  case class SensorData(sensorId: SensorId, timestamp: Timestamp, temperature: Option[Double], humidity: Option[Double])

  // Configuración de Spark Session
  val spark = SparkSession.builder
    .appName("IoT Farm Monitoring")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
    .config("spark.sql.extensions", extensions)
    .config("spark.sql.catalog.spark_catalog", sparkCatalog)
    .config("spark.sql.shuffle.partitions", shufflePartitions)
    .getOrCreate()

  spark.sparkContext.setLogLevel(logLevel)

  import spark.implicits._

  // Crear un acumulador para contar errores
  val errorCounter = spark.sparkContext.longAccumulator("ErrorCounter")

  // Leer la tabla estática de "zonas" desde un archivo JSON
  val zonasDF = spark.read
    .option("multiline", "true")
    .json(zonesJson)

  // Verificar que se han leído las zonas correctamente
  println("Zonas DF:")
  zonasDF.show(false)

  // Explode de las zonas y sensores para crear una tabla relacional
  val zonasExplodedDF = zonasDF
    .withColumn("zona", explode(col("zonas")))
    .select(
      col("zona.id").alias("zoneCode"),
      col("zona.nombre").alias("zoneId"),
      explode(col("zona.sensores")).alias("sensor")
    )
    .select(
      col("zoneCode"),
      col("zoneId"),
      col("sensor.id").alias("sensorCode"),
      col("sensor.nombre").alias("sensorId"),
      col("sensor.latitud").alias("sensorLatitude"),
      col("sensor.longitud").alias("sensorLongitude"),
      col("sensor.tipo").alias("sensorType")
    )

  // Verificar que se ha hecho el explode correctamente
  println("Zonas Exploded DF:")
  zonasExplodedDF.show(false)

  // Función para devolver un Dataset con una tupla de (valor, timestamp) desde Kafka
  def getKafkaStream(topic: String, spark: SparkSession): Dataset[(String, Timestamp)] = {
    import spark.implicits._
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
  }

  // Leer datos de Kafka para sensores de temperatura y humedad
  val temperatureHumidityDS: Dataset[SensorData] = getKafkaStream(temperatureHumidityTopic, spark).flatMap {
    case (value, timestamp) =>
      println(s"Raw data from Kafka: $value at $timestamp") // Depuración
      validarDatosSensorTemperatureHumidity(value, timestamp, errorCounter).map(th =>
        SensorData(th.sensorId, th.timestamp, Some(th.temperature), Some(th.humidity)))
  }

  // Añadir watermark para permitir agregaciones con datos tardíos hasta 5 minutos
  val withWatermarkDS = temperatureHumidityDS
    .withWatermark("timestamp", "5 minutes")

  // Verificar que se han leído los datos del sensor correctamente
  println("Temperature Humidity DS:")
  withWatermarkDS.printSchema()

  // Función para realizar el broadcast join y enriquecer los datos
  def enrichData(sensorDataDS: Dataset[SensorData], zonesDF: DataFrame): DataFrame = {
    sensorDataDS.join(
      broadcast(zonesDF),
      sensorDataDS("sensorId") === zonesDF("sensorId"),
      "left"
    ).select(
      sensorDataDS("sensorId"),
      sensorDataDS("timestamp"),
      sensorDataDS("temperature"),
      sensorDataDS("humidity"),
      col("zoneCode"),
      col("zoneId"),
      col("sensorLatitude"),
      col("sensorLongitude"),
      col("sensorType")
    )
  }

  // Definir el stream de consulta
  val query = withWatermarkDS.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .foreachBatch { (batchDS: Dataset[SensorData], batchId: Long) =>
      println(s"=== Batch $batchId ===")
      println("Datos leídos en el batch:")
      batchDS.show(false)

      val enrichedBatchDF = enrichData(batchDS, zonasExplodedDF)
      println("Query del join en el batch:")
      enrichedBatchDF.show(false)

      val avgTempDF = enrichedBatchDF
        .groupBy(
          window(col("timestamp"), "1 hour"),
          col("sensorId")
        )
        .agg(avg("temperature").alias("avg_temperature"))
        .select("window", "sensorId", "avg_temperature")

      println("Datos después de la agregación:")
      avgTempDF.show(false)

      println(s"Total de errores detectados hasta el momento: ${errorCounter.value}")
    }
    .start()

  // Esperar la finalización de la consulta
  query.awaitTermination()
}

