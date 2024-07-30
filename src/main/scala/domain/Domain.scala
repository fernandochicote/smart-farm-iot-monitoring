package domain

import main.SensorIdEnum.SensorId

import java.sql.Timestamp

object Domain {

  // Clase para representar los datos de un sensor de temperatura y humedad
  case class TemperatureHumidityData(sensorId: SensorId, temperature: Double, humidity: Double, timestamp: Timestamp)

  // Clase para representar todos los datos de los sensores
  case class SensorData(sensorId: SensorId, timestamp: Timestamp, temperature: Option[Double], humidity: Option[Double])

}

object Encoders {

}
