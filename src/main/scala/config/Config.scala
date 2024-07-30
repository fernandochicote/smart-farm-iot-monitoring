package config

object Config {

  // Configuración de Kafka
  val kafkaBootstrapServers = "localhost:9092"
  val temperatureHumidityTopic = "temperature_humidity"
  val co2Topic = "co2"
  val soilMoistureTopic = "soil_moisture"


  // Configuración de Spark
  val checkpointLocation = "./tmp/checkpoint"
  val extensions = "io.delta.sql.DeltaSparkSessionExtension"
  val sparkCatalog = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  val shufflePartitions = "10"
  val logLevel = "FATAL"

  // Rutas de directorios
  val rutaBase = "./tmp/"
  val sufijoCheckpoint ="_chk"
  val zonesJson = "./files/zones.json"


  object Tablas {
    val RawTemperatureHumidityZone = "raw_temperature_humidity_zone"
    val TemperatureHumidityZoneMerge = "temperature_humidity_zone_merge"
    val RawCo2Zone = "raw_c02_zone"
    val Co2ZoneMerge = "c02_zone_merge"
    val RawSoilMoistureZone = "raw_soil_moisture_zone"
    val SoilMoistureZoneMerge = "soil_moisture_zone_merge"
  }

  def getRutaParaTabla(nombreTabla: String): String = {
    rutaBase + nombreTabla
  }

  def getRutaParaTablaChk(nombreTabla: String): String = {

    getRutaParaTabla(nombreTabla) + sufijoCheckpoint
  }

}

object ConfigApp extends App {
  import Config.Tablas._
  import Config._

  // Ver test/scala/config/ConfigTest.scala
  println(getRutaParaTabla(RawTemperatureHumidityZone))
  //assert(getRutaParaTabla(RawTemperatureHumidityZone) == "./tmp/raw_temperature_humidity_zone")
  println(getRutaParaTablaChk(RawTemperatureHumidityZone))
  //assert(getRutaParaTablaChk(RawTemperatureHumidityZone) == "./tmp/raw_temperature_humidity_zone_chk")

  println(getRutaParaTabla(TemperatureHumidityZoneMerge))
  println(getRutaParaTablaChk(TemperatureHumidityZoneMerge))
}
