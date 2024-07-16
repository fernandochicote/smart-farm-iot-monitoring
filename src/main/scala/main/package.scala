import main.SensorIdEnum.SensorId
import main.ZoneIdEnum.ZoneId

import java.sql.Timestamp

package object main {



    // Clase para representar los datos de un sensor de humedad del suelo
    case class SoilMoistureData(sensorId: SensorId, soilMoisture: Double, timestamp: Timestamp)

    // Clase para representar los datos de un sensor de temperatura y humedad
    case class TemperatureHumidityData(sensorId: SensorId, temperature: Double, humidity: Double, timestamp: Timestamp, zoneId: Option[ZoneId] = None)

    // Clase para representar los datos de un sensor de nivel de CO2
    case class CO2Data(sensorId: SensorId, co2Level: Double, timestamp: Timestamp, zoneId: Option[ZoneId] = None)

  }



