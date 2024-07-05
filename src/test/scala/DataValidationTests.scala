import Main.{CO2Data, SoilMoistureData, TemperatureHumidityData}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import org.scalatest.BeforeAndAfterAll
import com.github.mrpowers.spark.fast.tests.DatasetComparer

class DataValidationTests extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer {

  test("Testing validarDatosSensorTemperatureHumidity") {
    val value = "sensor1,12.12,22.22"
    val timestamp = new Timestamp(System.currentTimeMillis())
    val result = DataValidations.validarDatosSensorTemperatureHumidity(value, timestamp)
    val expected = TemperatureHumidityData("sensor1", 12.12, 22.22, timestamp)

    assert(result == expected)
  }

  test("Testing validarDatosSensorTemperatureHumiditySoilMoisture") {
    val value = "sensor1,13.13,2022-10-20 10:20:30.0"
    val timestamp = Timestamp.valueOf("2022-10-20 10:20:30.0")
    val result = DataValidations.validarDatosSensorTemperatureHumiditySoilMoisture(value, timestamp)
    val expected = SoilMoistureData("sensor1", 13.13, timestamp)

    assert(result == expected)
  }

  test("Testing validarDatosSensorCO2") {
    val value = "sensor1,14.14,2022-10-20 10:20:30.0"
    val timestamp = Timestamp.valueOf("2022-10-20 10:20:30.0")
    val result = DataValidations.validarDatosSensorCO2(value, timestamp)
    val expected = CO2Data("sensor1", 14.14, timestamp)

    assert(result == expected)
  }
}
