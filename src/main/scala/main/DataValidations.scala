package main

import main.SensorIdEnum.{Sensor1, Sensor2, Sensor3, Sensor4, Sensor5, Sensor6, Sensor7, Sensor8, Sensor9, SensorId, Unknown}

import java.sql.Timestamp
import scala.util.Try

object DataValidations {

  private def sensorIdEnumFromString(value: String): Option[SensorId] = {
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
