import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import org.scalatest.BeforeAndAfterAll
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import main.{CO2Data, DataValidations, SensorIdEnum, SoilMoistureData, TemperatureHumidityData}

class DataValidationTests extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer {

  test("Testing validarDatosSensorTemperatureHumidity") {
    val value = "sensor1,12.12,22.22"
    val timestamp = new Timestamp(System.currentTimeMillis())
    val result = DataValidations.validarDatosSensorTemperatureHumidity(value, timestamp)
    val expected = Some(TemperatureHumidityData(SensorIdEnum.Sensor1, 12.12, 22.22, timestamp))

    assert(result == expected)
  }

  test("validarDatosSensorTemperatureHumidity should return None for invalid input") {
    val input = "sensor1,invalid,65.0"
    val timestamp = Timestamp.valueOf("2023-10-01 12:00:00")
    val result = DataValidations.validarDatosSensorTemperatureHumidity(input, timestamp)
    assert(result.isEmpty)
  }

  test("Testing validarDatosSensorTemperatureHumiditySoilMoisture") {
    val value = "sensor1,13.13,2022-10-20 10:20:30.0"
    val timestamp = Timestamp.valueOf("2022-10-20 10:20:30.0")
    val result = DataValidations.validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp)
    val expected = Some(SoilMoistureData(SensorIdEnum.Sensor1, 13.13, timestamp))

    assert(result == expected)
  }

  test("validarDatosSensorTemperatureHumiditySoilMoisture should return None for invalid input") {
    val input = "sensor2,30.2,invalid"
    val timestamp = Timestamp.valueOf("2023-10-01 12:00:00")
    val result = DataValidations.validarDatosSensorTemperatureHumiditySoilMoisture(input, timestamp)
    assert(result.isEmpty)
  }

  test("Testing validarDatosSensorCO2") {
    val value = "sensor1,14.14,2022-10-20 10:20:30.0"
    val timestamp = Timestamp.valueOf("2022-10-20 10:20:30.0")
    val result = DataValidations.validarDatosSensorCO2(value, timestamp)
    val expected = Some(CO2Data(SensorIdEnum.Sensor1, 14.14, timestamp))

    assert(result == expected)
  }

  test("validarDatosSensorCO2 should return None for invalid input") {
    val input = "sensor3,invalid,2023-10-01 12:00:00"
    val timestamp = Timestamp.valueOf("2023-10-01 12:00:00")
    val result = DataValidations.validarDatosSensorCO2(input, timestamp)
    assert(result.isEmpty)
  }
}
