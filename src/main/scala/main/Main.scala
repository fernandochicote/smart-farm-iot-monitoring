package main

// Importaciones de Spark
import config.Config
import config.Config._
import main.DataValidations.{validarDatosSensorCO2, validarDatosSensorTemperatureHumidity, validarDatosSensorTemperatureHumiditySoilMoisture}
import main.Main.{CO2Data, SensorData, SoilMoistureData, TemperatureHumidityData}
import main.SensorIdEnum._
import main.ZoneIdEnum._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct, udf}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

// Importaciones estándar de Java y Scala
import java.sql.Timestamp
import scala.util.Try

object DataValidations {

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

  def validarDatosSensorTemperatureHumidity(value: String, timestamp: Timestamp): Option[TemperatureHumidityData] = {
    val parts = value.split(",")
    val sensorIdInt = sensorIdEnumFromString(parts(0)).get

    if (parts.length == 4) {  // Asegúrate de que el número de partes coincida con el formato del mensaje
      for {
        temperature <- toDouble(parts(1))
        humidity <- toDouble(parts(2))
      } yield TemperatureHumidityData(sensorIdInt, temperature, humidity, timestamp)
    } else None
  }

  def validarDatosSensorTemperatureHumiditySoilMoisture(value: String, timestamp: Timestamp): Option[SoilMoistureData] = {
    val parts = value.split(",")
    val sensorIdInt = sensorIdEnumFromString(parts(0)).get
    if (parts.length == 3) {
      for {
        moisture <- toDouble(parts(1))
      } yield SoilMoistureData(sensorIdInt, moisture, timestamp)
    } else None
  }

  def validarDatosSensorCO2(value: String, timestamp: Timestamp): Option[CO2Data] = {
    val parts = value.split(",")
    val sensorIdInt = sensorIdEnumFromString(parts(0)).get
    if (parts.length == 3) {
      for {
        co2 <- toDouble(parts(1))
      } yield CO2Data(sensorIdInt, co2, timestamp)
    } else None
  }

  private def toDouble(value: String): Option[Double] = Try(value.toDouble).toOption

  private def toTimestamp(value: String): Option[Timestamp] = Try(Timestamp.valueOf(value)).toOption

}

object Main extends App {

  // Clase para representar los datos de un sensor de humedad del suelo
  case class SoilMoistureData(sensorId: SensorId, soilMoisture: Double, timestamp: Timestamp)

  // Clase para representar los datos de un sensor de temperatura y humedad
  case class TemperatureHumidityData(sensorId: SensorId, temperature: Double, humidity: Double, timestamp: Timestamp, zoneId: Option[ZoneId] = None)

  // Clase para representar los datos de un sensor de nivel de CO2
  case class CO2Data(sensorId: SensorId, co2Level: Double, timestamp: Timestamp, zoneId: Option[ZoneId] = None)

  // Clase para representar todos los datos de los sensores
  case class SensorData(sensorId: SensorId, timestamp: Timestamp, zoneId: Option[ZoneId], sensorType: String, temperature: Option[Double], humidity: Option[Double], co2Level: Option[Double], soilMoisture: Option[Double])

  // UDF para obtener zoneId
  val sensorIdToZoneId: UserDefinedFunction = udf((sensorId: String) => {
    Try(SensorIdEnum.withName(sensorId)).toOption.flatMap(sensorToZoneMap.get).map(_.toString).getOrElse("unknown")
  })

  // Devuelve un Dataset con una tupla de (valor, timestamp), donde el campo valor es un string
  def getKafkaStream(topic: String, spark: SparkSession) = {
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

  // Configuración de Spark Session
  val spark = SparkSession.builder
    .appName("IoT Farm Monitoring")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
    .config("spark.sql.extensions", extensions)
    .config("spark.sql.catalog.spark_catalog", sparkCatalog)
    // Shuffle partitions
    .config("spark.sql.shuffle.partitions", shufflePartitions)
    .getOrCreate()

  spark.sparkContext.setLogLevel(logLevel)

  import spark.implicits._

  // Mapeo de sensores a zonas
  private val sensorToZoneMap: Map[SensorId, ZoneId] = Map(
    Sensor1 -> Zone1,
    Sensor2 -> Zone1,
    Sensor3 -> Zone1,
    Sensor4 -> Zone2,
    Sensor5 -> Zone2,
    Sensor6 -> Zone2,
    Sensor7 -> Zone3,
    Sensor8 -> Zone3,
    Sensor9 -> Zone3
  )

  // Leer datos de Kafka para todos los sensores
  val temperatureHumidityDS: Dataset[SensorData] = getKafkaStream(temperatureHumidityTopic, spark).flatMap {
    case (value, timestamp) =>
      validarDatosSensorTemperatureHumidity(value, timestamp).map(th =>
        SensorData(th.sensorId, th.timestamp, sensorToZoneMap.get(th.sensorId), "TemperatureHumidity", Some(th.temperature), Some(th.humidity), None, None))
  }

  val co2DS: Dataset[SensorData] = getKafkaStream(co2Topic, spark).flatMap {
    case (value, timestamp) =>
      validarDatosSensorCO2(value, timestamp).map(co2 =>
        SensorData(co2.sensorId, co2.timestamp, sensorToZoneMap.get(co2.sensorId), "CO2", None, None, Some(co2.co2Level), None))
  }

  val soilMoistureDS: Dataset[SensorData] = getKafkaStream(soilMoistureTopic, spark).flatMap {
    case (value, timestamp) =>
      validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp).map(sm =>
        SensorData(sm.sensorId, sm.timestamp, sensorToZoneMap.get(sm.sensorId), "SoilMoisture", None, None, None, Some(sm.soilMoisture)))
  }

  // Unir todos los datasets en un solo dataframe
  val unifiedDF = temperatureHumidityDS
    .unionByName(co2DS)
    .unionByName(soilMoistureDS)

  // Mostrar en consola los datos que llegan
  val unifiedQuery = unifiedDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  // Esperar la finalización de la consulta
  unifiedQuery.awaitTermination()
}

