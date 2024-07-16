package main

import DataValidations.{validarDatosSensorCO2, validarDatosSensorTemperatureHumidity, validarDatosSensorTemperatureHumiditySoilMoisture}
import config.Config
import config.Config._
import main.AppHelperFunctions.{getKafkaStream, sensorIdToZoneId}
import main.SensorIdEnum._
import main.ZoneIdEnum._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, col, udf, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import utils.util.PrintUtils
import DomainEncoders._
import java.sql.Timestamp
import scala.util.Try

object AppHelperFunctions {

  val sensorToZoneMap: Map[SensorId, ZoneId] = Map(
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

  val sensorIdToZoneId: UserDefinedFunction = udf((sensorId: String) => {
    Try(SensorIdEnum.withName(sensorId)).toOption.flatMap(sensorToZoneMap.get).map(_.toString).getOrElse("unknown")
  })

  def getKafkaStream(topic: String)(implicit spark: SparkSession): Dataset[(String, Timestamp)] = {
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
}

object Main extends App with PrintUtils {

  printBoldMessage("Starting IoT Farm Monitoring")

  implicit val spark: SparkSession = SparkSession.builder
    .appName("IoT Farm Monitoring")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", checkpointLocation)
    .config("spark.sql.extensions", extensions)
    .config("spark.sql.catalog.spark_catalog", sparkCatalog)
    .config("spark.sql.shuffle.partitions", shufflePartitions)
    .getOrCreate()

  spark.sparkContext.setLogLevel(logLevel)

  import spark.implicits._

  // Leer datos de Kafka para todos los sensores
  printBoldMessage("Reading data from Kafka: temperature_humidity")
  val temperatureHumidityDS: Dataset[Option[TemperatureHumidityData]] = getKafkaStream(temperatureHumidityTopic).map {
    case (value, timestamp) =>
      validarDatosSensorTemperatureHumidity(value, timestamp)
  }

  printBoldMessage("Reading data from Kafka: co2")
  val co2DS: Dataset[Option[CO2Data]] = getKafkaStream(co2Topic).map {
    case (value, timestamp) =>
      validarDatosSensorCO2(value, timestamp)
  }

  printBoldMessage("Reading data from Kafka: soil_moisture")
  val soilMoistureDS: Dataset[Option[SoilMoistureData]] = getKafkaStream(soilMoistureTopic).map {
    case (value, timestamp) =>
      validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp)
  }

  // Filtrar None y extraer valores de Some, asignar zona a cada sensor y convertir a DataFrame
  def assignZoneAndConvertToDF[T <: SensorData](ds: Dataset[Option[T]])(implicit encoder: Encoder[T]): DataFrame = {
    ds.flatMap(_.toSeq)
      .map(data => data.copy(zoneId = AppHelperFunctions.sensorToZoneMap.get(data.sensorId)))
      .toDF()
  }


  val temperatureHumidityDFWithZone = assignZoneAndConvertToDF(temperatureHumidityDS)
  val co2DFWithZone = assignZoneAndConvertToDF(co2DS)(co2DataEncoder)
  val soilMoistureDFWithZone = assignZoneAndConvertToDF(soilMoistureDS)(soilMoistureDataEncoder)

  // Creación de tablas para cada sensor
  val temperatureHumiditySchema = temperatureHumidityDFWithZone.schema
  val co2Schema = co2DFWithZone.schema
  val soilMoistureSchema = soilMoistureDFWithZone.schema

  val tHemptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], temperatureHumiditySchema)
  val co2emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], co2Schema)
  val sMemptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], soilMoistureSchema)

  tHemptyDF.write
    .mode("overwrite")
    .format("delta")
    .save(getRutaParaTabla(Config.Tablas.RawTemperatureHumidityZone))

  // Fernando: Si te fijas como tu dataset es un Dataset[Option[TemperatureHumidityData]],
  tHemptyDF.toDF().printSchema()

  tHemptyDF.toDF().write
    .mode("overwrite")
    .format("delta")
    .partitionBy("zoneId", "sensorId")
    .save(getRutaParaTabla(Config.Tablas.TemperatureHumidityZoneMerge))

  co2emptyDF.write
    .mode("overwrite")
    .format("delta")
    .save(getRutaParaTabla(Config.Tablas.RawCo2Zone))

  co2emptyDF.write
    .mode("overwrite")
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

  // Procesar y agregar datos de temperatura y humedad
  def processSensorData(df: DataFrame, sensorField: String, watermarkDuration: String, windowDuration: String): DataFrame = {
    df.filter($"zoneId" =!= "unknown")
      .withWatermark("timestamp", watermarkDuration)
      .groupBy(window($"timestamp".cast("timestamp"), windowDuration), $"zoneId")
      .agg(avg(col(sensorField)).as(s"avg_$sensorField"))
      .toDF()
  }

  val avgTemperatureDF = processSensorData(temperatureHumidityDFWithZone, "temperature", "1 minute", "1 minute")
  val avgCo2DF = processSensorData(co2DFWithZone, "co2Level", "1 minute", "1 minute")
  val avgSoilMoistureDF = processSensorData(soilMoistureDFWithZone, "soilMoisture", "1 minute", "1 minute")

  def writeToConsole(df: DataFrame, outputMode: String, format: String, triggerDuration: String, truncate: Boolean = false): StreamingQuery = {
    df.writeStream
      .outputMode(outputMode)
      .format(format)
      .option("truncate", truncate.toString)
      .trigger(Trigger.ProcessingTime(triggerDuration))
      .start()
  }

  val tempQuery = writeToConsole(avgTemperatureDF, "complete", "console", "10 seconds")
  val co2Query = writeToConsole(avgCo2DF, "complete", "console", "10 seconds")
  val soilQuery = writeToConsole(avgSoilMoistureDF, "complete", "console", "10 seconds")

  val unMappedDevicesDF = temperatureHumidityDFWithZone.filter($"zoneId" === "unknown")
  val unmappedQuery = writeToConsole(unMappedDevicesDF, "append", "console", "20 seconds")

  def awaitTermination(queries: List[StreamingQuery]): Unit = {
    queries.foreach(_.awaitTermination())
  }

  awaitTermination(List(tempQuery, co2Query, soilQuery, unmappedQuery))
}