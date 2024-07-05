package config

object Config {

  // Configuración de Kafka
  val kafkaBootstrapServers = "localhost:9092"
  val temperatureHumidityTopic = "temperature_humidity"
  val co2Topic = "co2"
  val soilMoistureTopic = "soil_moisture"

  // Rutas de directorios
  val rutaBase = "./tmp/"
  val sufijoCheckpoint ="_chk"


  object Tablas {
    val RawTemperatureHumidityZone = "raw_temperature_humidity_zone"
    val TemperatureHumidityZoneMerge = "temperature_humidity_zone_merge"
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
