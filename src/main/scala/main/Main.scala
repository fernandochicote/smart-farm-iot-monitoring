package main

// Importaciones de Spark

import config.Config
import config.Config._
import main.DataValidations.{validarDatosSensorCO2, validarDatosSensorTemperatureHumidity, validarDatosSensorTemperatureHumiditySoilMoisture}
import main.Main.{CO2Data, SoilMoistureData, TemperatureHumidityData}
import main.SensorIdEnum._
import main.ZoneIdEnum._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, col, udf, window}
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

    if (parts.length == 3) {
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
        ts <- toTimestamp(parts(2))
      } yield SoilMoistureData(sensorIdInt, moisture, ts)
    } else None
  }

  def validarDatosSensorCO2(value: String, timestamp: Timestamp): Option[CO2Data] = {
    val parts = value.split(",")
    val sensorIdInt = sensorIdEnumFromString(parts(0)).get
    if (parts.length == 3) {
      for {
        co2 <- toDouble(parts(1))
        ts <- toTimestamp(parts(2))
      } yield CO2Data(sensorIdInt, co2, ts)
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


  val temperatureHumidityDS: Dataset[TemperatureHumidityData] = getKafkaStream(temperatureHumidityTopic, spark).flatMap {
    case (value, timestamp) =>
      validarDatosSensorTemperatureHumidity(value, timestamp)
  }

  val co2DS: Dataset[CO2Data] = getKafkaStream(co2Topic, spark).flatMap {
    case (value, timestamp) =>
      validarDatosSensorCO2(value, timestamp)
  }

  val soilMoistureDS: Dataset[SoilMoistureData] = getKafkaStream(soilMoistureTopic, spark).flatMap {
    case (value, timestamp) =>
      validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp)
  }

  // Asignar zona a cada sensor

  val temperatureHumidityDFWithZone = temperatureHumidityDS.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))
  val co2DFWithZone = co2DS.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))
  val soilMoistureDFWithZone = soilMoistureDS.withColumn("zoneId", sensorIdToZoneId(col("sensorId")))


  // Creacion de tablas para cada sensor
  // TODO: hacerlo modular --> Por un lado funcion para crear tabla, por otro escritura

  val temperatureHumiditySchema = temperatureHumidityDFWithZone.schema
  val co2Schema = co2DFWithZone.schema
  val soilMoistureSchema = soilMoistureDFWithZone.schema

  val tHemptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], temperatureHumiditySchema)
  val co2emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], co2Schema)
  val sMemptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], soilMoistureSchema)


  tHemptyDF.write
    .format("delta")
    .save(getRutaParaTabla(Config.Tablas.RawTemperatureHumidityZone))

  tHemptyDF.write
    .format("delta")
    .partitionBy("zoneId", "sensorId")
    .save(getRutaParaTabla(Config.Tablas.TemperatureHumidityZoneMerge))

  co2emptyDF.write
    .format("delta")
    .save(getRutaParaTabla(Config.Tablas.RawCo2Zone))

  co2emptyDF.write
    .format("delta")
    .partitionBy("zoneId", "sensorId")
    .save(getRutaParaTabla(Config.Tablas.Co2ZoneMerge))

  sMemptyDF.write
    .format("delta")
    .save(getRutaParaTabla(Config.Tablas.RawSoilMoistureZone))

  sMemptyDF.write
    .format("delta")
    .partitionBy("zoneId", "sensorId")
    .save(getRutaParaTabla(Config.Tablas.SoilMoistureZoneMerge))

  // Escritura de las tablas de las tablas en streaming
  temperatureHumidityDFWithZone.writeStream
    .format("delta")
    .option("checkpointLocation", getRutaParaTablaChk(Config.Tablas.RawTemperatureHumidityZone))
    .trigger(Trigger.ProcessingTime("5 second"))
    .start(getRutaParaTabla(Config.Tablas.RawTemperatureHumidityZone))

  // Lectura de las tablas de las tablas en streaming

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
    .load(getRutaParaTabla(Config.Tablas.TemperatureHumidityZoneMerge))
    .coalesce(1)
    .writeStream
    .outputMode("append")
    .format("json")
    .start("./tmp/temperature_humidity_zone_merge_json")

  //TODO: funcion para procesar datos en streaming

  // Procesamiento y agregación de datos en tiempo real (Ejemplo: Promedio por minuto)

  // Función para filtrar y agregar datos en tiempo real
  def processSensorData(df: DataFrame, sensorField: String, watermarkDuration: String, windowDuration: String): DataFrame = {
    df.filter($"zoneId" =!= "unknown")
      .withWatermark("timestamp", watermarkDuration)
      .groupBy(window($"timestamp".cast("timestamp"), windowDuration), $"zoneId")
      .agg(avg(col(sensorField)).as(s"avg_$sensorField"))
  }

  // Procesar y agregar datos de temperatura y humedad
  val avgTemperatureDF = processSensorData(temperatureHumidityDFWithZone, "temperature", "1 minute", "1 minute")
  val avgCo2DF = processSensorData(co2DFWithZone, "co2Level", "1 minute", "1 minute")
  val avgSoilMoistureDF = processSensorData(soilMoistureDFWithZone, "soilMoisture", "1 minute", "1 minute")

  // Función para escribir los resultados en la consola
  def writeToConsole(df: DataFrame, outputMode: String, format: String, triggerDuration: String, truncate: Boolean = false): StreamingQuery = {
    df.writeStream
      .outputMode(outputMode)
      .format(format)
      .option("truncate", truncate.toString)
      .trigger(Trigger.ProcessingTime(triggerDuration))
      .start()
  }

  // Escribir resultados en la consola y obtener las consultas
  val tempQuery = writeToConsole(avgTemperatureDF, "complete", "console", "10 seconds")
  val co2Query = writeToConsole(avgCo2DF, "complete", "console", "10 seconds")
  val soilQuery = writeToConsole(avgSoilMoistureDF, "complete", "console", "10 seconds")

  // Mostrar los dispositivos que no están mapeados a una zona
  val unMappedDevicesDF = temperatureHumidityDFWithZone.filter($"zoneId" === "unknown")
  val unmappedQuery = writeToConsole(unMappedDevicesDF, "append", "console", "20 seconds")

  // Función para esperar la finalización de las consultas de streaming
  def awaitTermination(queries: List[StreamingQuery]): Unit = {
    queries.foreach(_.awaitTermination())
  }

  // Esperar la finalización de todas las consultas
  awaitTermination(List(tempQuery, co2Query, soilQuery, unmappedQuery))

}