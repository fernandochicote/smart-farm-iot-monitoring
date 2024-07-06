// Importaciones de Spark
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.UserDefinedFunction


// Importaciones estándar de Java y Scala
import java.sql.Timestamp
import scala.util.Try

// Importaciones del proyecto
import config.Config
import config.Config._
import DataValidations.{validarDatosSensorCO2, validarDatosSensorTemperatureHumidity, validarDatosSensorTemperatureHumiditySoilMoisture}
import Main.{CO2Data, SoilMoistureData, TemperatureHumidityData}
import SensorId._
import ZoneId._

// Enumeraciones para sensorId
object SensorId extends Enumeration {
  type SensorId = Value
  val Sensor1, Sensor2, Sensor3, Sensor4, Sensor5, Sensor6, Sensor7, Sensor8, Sensor9 = Value
}

// Enumeraciones para zoneId
object ZoneId extends Enumeration {
  type ZoneId = Value
  val Zone1, Zone2, Zone3 = Value
}


object DataValidations {

  def validarDatosSensorTemperatureHumidity(value: String, timestamp: Timestamp): Option[TemperatureHumidityData] = {
    val parts = value.split(",")
    if (parts.length == 3) {
      for {
        sensorId <- toSensorId(parts(0))
        temperature <- toDouble(parts(1))
        humidity <- toDouble(parts(2))
      } yield TemperatureHumidityData(sensorId, temperature, humidity, timestamp)
    } else None
  }

  def validarDatosSensorTemperatureHumiditySoilMoisture(value: String, timestamp: Timestamp): Option[SoilMoistureData] = {
    val parts = value.split(",")
    if (parts.length == 3) {
      for {
        sensorId <- toSensorId(parts(0))
        moisture <- toDouble(parts(1))
        ts <- toTimestamp(parts(2))
      } yield SoilMoistureData(sensorId, moisture, ts)
    } else None
  }

  def validarDatosSensorCO2(value: String, timestamp: Timestamp): Option[CO2Data] = {
    val parts = value.split(",")
    if (parts.length == 3) {
      for {
        sensorId <- toSensorId(parts(0))
        co2 <- toDouble(parts(1))
        ts <- toTimestamp(parts(2))
      } yield CO2Data(sensorId, co2, ts)
    } else None
  }

  private def toDouble(value: String): Option[Double] = Try(value.toDouble).toOption

  private def toTimestamp(value: String): Option[Timestamp] = Try(Timestamp.valueOf(value)).toOption

  private def toSensorId(value: String): Option[SensorId.Value] = Try(SensorId.withName(value)).toOption


}

object Main extends App {

  import spark.implicits._


  // Clase para representar los datos de un sensor de humedad del suelo
  case class SoilMoistureData(sensorId: SensorId, soilMoisture: Double, timestamp: Timestamp)

  // Clase para representar los datos de un sensor de temperatura y humedad
  case class TemperatureHumidityData(sensorId: SensorId, temperature: Double, humidity: Double, timestamp: Timestamp, zoneId: Option[ZoneId] = None)

  // Clase para representar los datos de un sensor de nivel de CO2
  case class CO2Data(sensorId: SensorId, co2Level: Double, timestamp: Timestamp, zoneId: Option[ZoneId] = None)

  // UDF para obtener zoneId
  val sensorIdToZoneId: UserDefinedFunction = udf((sensorId: String) => {
    Try(SensorId.withName(sensorId)).toOption.flatMap(sensorToZoneMap.get).map(_.toString).getOrElse("unknown")
  })



  // Devuelve un Dataset con una tupla de (valor, timestamp), donde el campo valor es un string
  def getKafkaStream(topic: String , spark: SparkSession) = {
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
    .config("spark.sql.streaming.checkpointLocation", "./tmp/checkpoint")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Shuffle partitions
    .config("spark.sql.shuffle.partitions", "10")
    .getOrCreate()

  spark.sparkContext.setLogLevel("FATAL")

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

  // Leer datos de Kafka para temperatura y humedad

  val temperatureHumidityDF: Dataset[TemperatureHumidityData] = getKafkaStream(temperatureHumidityTopic, spark).flatMap {

    case (value, timestamp) =>
      validarDatosSensorTemperatureHumidity(value, timestamp)

  }


  val temperatureHumidityDFWithZone = temperatureHumidityDF.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))

  val schema = temperatureHumidityDFWithZone.schema
  val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

  emptyDF.write
    .format("delta")
    //.save("./tmp/raw_temperature_humidity_zone")
    .save(getRutaParaTabla(Config.Tablas.RawTemperatureHumidityZone))



  /*
  emptyDF.write
    .format("json")
    .save("./tmp/temperature_humidity_zone_merge_json")
*/
  emptyDF.write
    .format("delta")
    .partitionBy("zoneId", "sensorId")
    //.save("./tmp/temperature_humidity_zone_merge")
    .save(getRutaParaTabla(Config.Tablas.TemperatureHumidityZoneMerge))



  temperatureHumidityDFWithZone.writeStream
    .format("delta")
    .option("checkpointLocation", getRutaParaTablaChk(Config.Tablas.RawTemperatureHumidityZone))
    .trigger(Trigger.ProcessingTime("5 second"))
    .start(getRutaParaTabla(Config.Tablas.RawTemperatureHumidityZone))

  spark.readStream
    .format("delta")
    .load(getRutaParaTabla(Config.Tablas.RawTemperatureHumidityZone))
    .coalesce(1)
    .writeStream
    .option("mergeSchema", "true")
    .outputMode("append")
    .partitionBy("zoneId", "sensorId")
    .format("delta")
    .option("checkpointLocation", "./tmp/temperature_humidity_zone_merge_chk")
    .trigger(Trigger.ProcessingTime("60 second"))
    .start("./tmp/temperature_humidity_zone_merge")

  spark.readStream
    .format("delta")
    .load("./tmp/temperature_humidity_zone_merge")
    .coalesce(1)
    .writeStream
    .outputMode("append")
    .format("json")
    //.partitionBy("zoneId", "sensorId")
    .start("./tmp/temperature_humidity_zone_merge_json")


  // Procesamiento y agregación de datos en tiempo real (Ejemplo: Promedio de temperatura por minuto)
  val avgTemperatureDF = temperatureHumidityDFWithZone
    .filter($"zoneId" =!= "unknown")
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window($"timestamp".cast("timestamp"), "1 minute"),
      $"zoneId"
    )
    .agg(avg($"temperature").as("avg_temperature"))

  // Escribir resultados en la consola (puede ser almacenado en otro sistema)
  val query = avgTemperatureDF.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("10 second"))
    .start()

  // Mostrar los dispositivos que no están mapeados a una zona
  temperatureHumidityDFWithZone.filter($"zoneId" === "unknown")
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .trigger(Trigger.ProcessingTime("20 second"))
    .start()




  val co2DF = getKafkaStream(co2Topic, spark).map {
    case (value, timestamp) =>
      validarDatosSensorCO2(value, timestamp)
  }

  val avgCo2DF = co2DF
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window($"timestamp".cast("timestamp"), "1 minute"),
      $"sensorId"
    )
    .agg(avg($"co2Level").as("avg_co2Level"))

  val soilMoistureDF = getKafkaStream(soilMoistureTopic, spark).map {
    case (value, timestamp) =>
      // TODO: Qué tal si movemos esto a una función?
      validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp)
  }
  val avgSolilMoistureDF = soilMoistureDF
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window($"timestamp".cast("timestamp"), "1 minute"),
      $"sensorId"
    )
    .agg(avg($"soilMoisture").as("avg_soilMoisture"))


  // Unificar los datos de los diferentes sensores
  //val unifiedData = ???

  query.awaitTermination()

}