import main.SensorIdEnum.SensorId
import main.ZoneIdEnum.ZoneId
import org.apache.spark.sql.{Encoder, Encoders}

import java.sql.Timestamp

package object main {

    object DomainEncoders {
        // Encoders para las clases de dominio
        implicit val sensorDataEncoder: Encoder[SensorData] = Encoders.kryo[SensorData]

        implicit val soilMoistureDataEncoder: Encoder[SoilMoistureData] = Encoders.product[SoilMoistureData]
        implicit val optionSoilMoistureDataEncoder: Encoder[Option[SoilMoistureData]] = Encoders.kryo[Option[SoilMoistureData]]

        implicit val temperatureHumidityDataEncoder: Encoder[TemperatureHumidityData] = Encoders.product[TemperatureHumidityData]
        implicit val optionTemperatureHumidityDataEncoder: Encoder[Option[TemperatureHumidityData]] = Encoders.kryo[Option[TemperatureHumidityData]]

        implicit val co2DataEncoder: Encoder[CO2Data] = Encoders.product[CO2Data]
        implicit val optionCo2DataEncoder: Encoder[Option[CO2Data]] = Encoders.kryo[Option[CO2Data]]

    }

    // Superclase para representar los datos comunes de un sensor

    class SensorData(val sensorId: SensorId, timestamp: Timestamp, zoneId: Option[ZoneId] = None){
        def copy(sensorId: SensorId = this.sensorId, timestamp: Timestamp = this.timestamp, zoneId: Option[ZoneId] = this.zoneId): SensorData = {
            new SensorData(sensorId, timestamp, zoneId)
        }
    }

    // Clases específicas para cada tipo de sensor extendiendo desde la superclase SensorData
    // Creamos ya los encoders para cada una de las clases
    case class SoilMoistureData(  override val sensorId: SensorId, soilMoisture: Double,   timestamp: Timestamp,   zoneId: Option[ZoneId] = None) extends SensorData(sensorId, timestamp, zoneId)

    case class TemperatureHumidityData(  override val sensorId: SensorId, temperature: Double, humidity: Double,  timestamp: Timestamp,  zoneId: Option[ZoneId] = None) extends SensorData(sensorId, timestamp, zoneId)

    case class CO2Data(  override val sensorId: SensorId, co2Level: Double,   timestamp: Timestamp,   zoneId: Option[ZoneId] = None) extends SensorData(sensorId, timestamp, zoneId)
}



