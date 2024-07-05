import DataValidations.{validarDatosSensorCO2, validarDatosSensorTemperatureHumidity, validarDatosSensorTemperatureHumiditySoilMoisture}
import Main.{CO2Data, SoilMoistureData, TemperatureHumidityData}
import config.Config
import org.apache.spark.sql.{Dataset, Row}

import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql.DataFrame



object DataValidations {

  def validarDatosSensorTemperatureHumidity(value: String, timestamp: Timestamp): Option[TemperatureHumidityData] = {
    val parts = value.split(",")
    if (parts.length == 3) {
      for {
        temperature <- toDouble(parts(1))
        humidity <- toDouble(parts(2))
      } yield TemperatureHumidityData(parts(0), temperature, humidity, timestamp)
    } else None
  }

  def validarDatosSensorTemperatureHumiditySoilMoisture(value: String, timestamp: Timestamp): Option[SoilMoistureData] = {
    val parts = value.split(",")
    if (parts.length == 3) {
      for {
        moisture <- toDouble(parts(1))
        ts <- toTimestamp(parts(2))
      } yield SoilMoistureData(parts(0), moisture, ts)
    } else None
  }

  def validarDatosSensorCO2(value: String, timestamp: Timestamp): Option[CO2Data] = {
    val parts = value.split(",")
    if (parts.length == 3) {
      for {
        co2 <- toDouble(parts(1))
        ts <- toTimestamp(parts(2))
      } yield CO2Data(parts(0), co2, ts)
    } else None
  }

  private def toDouble(value: String): Option[Double] = Try(value.toDouble).toOption

  private def toTimestamp(value: String): Option[Timestamp] = Try(Timestamp.valueOf(value)).toOption

}

object Main extends App {

  import config.Config._
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.streaming.Trigger

  case class SensorData(sensorId: String, value: Double, timestamp: Timestamp)


  // Clase para representar los datos de un sensor de humedad del suelo
  case class SoilMoistureData(sensorId: String, soilMoisture: Double, timestamp: Timestamp)


  // Clase para representar los datos de un sensor de temperatura y humedad
  case class TemperatureHumidityData(sensorId: String, temperature: Double, humidity: Double, timestamp: Timestamp, zoneId: Option[String] = None)

  // Clase para representar los datos de un sensor de nivel de CO2
  case class CO2Data(sensorId: String, co2Level: Double, timestamp: Timestamp, zoneId: Option[String] = None)

  val sensorIdToZoneId = udf((sensorId: String) => sensorToZoneMap.getOrElse(sensorId, "unknown"))


  def readData(path: String, format: String)(implicit spark: SparkSession) =
    spark.readStream.format(format).load(path)

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

  def handleSensorData(df: Dataset[(String, Timestamp)], dataCaseClass: Function1[(String, Timestamp), SensorData])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val sensorDataDf = df.map(dataCaseClass)
    sensorDataDf.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))
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

  spark
    .sparkContext.setLogLevel("ERROR")

  import spark.implicits._




  // Mapeo de sensores a zonas
  // Ejemplo: sensor1 -> zona1, sensor2 -> zona2
  private type SensorId = String
  private type ZoneId   = String

  val Zone1: ZoneId = "zona1"

  // Identifica la zona a la que pertenece un dispositivo
  private val sensorToZoneMap: Map[SensorId, ZoneId] = Map(
    "sensor1" -> Zone1,
    "sensor2" -> Zone1,
    "sensor3" -> Zone1,
    "sensor4" -> "zone2",
    "sensor5" -> "zone2",
    "sensor6" -> "zone2",
    "sensor7" -> "zone3",
    "sensor8" -> "zone3",
    "sensor9" -> "zone3")

  // Leer datos de Kafka para temperatura y humedad

  val temperatureHumidityDF: Dataset[TemperatureHumidityData] = getKafkaStream(temperatureHumidityTopic, spark).map {

    case (value, timestamp) => {
      validarDatosSensorTemperatureHumidity(value, timestamp)

    }
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