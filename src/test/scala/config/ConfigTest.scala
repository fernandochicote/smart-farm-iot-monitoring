package config;

import org.scalatest.funsuite.AnyFunSuite

class ConfigTest extends AnyFunSuite {
  import config.Config._

    test("Kafka Bootstrap Servers") {
        assert(kafkaBootstrapServers == "localhost:9092")
    }

    test("Temperature Humidity Topic") {
        assert(temperatureHumidityTopic == "temperature_humidity")
    }

    test("CO2 Topic") {
        assert(co2Topic == "co2")
    }

    test("Soil Moisture Topic") {
        assert(soilMoistureTopic == "soil_moisture")
    }

    test("Ruta Base") {
        assert(rutaBase == "./tmp/")
    }

    test("Sufijo Checkpoint") {
        assert(sufijoCheckpoint == "_chk")
    }

    test("Tablas") {
        assert(Tablas.RawTemperatureHumidityZone == "raw_temperature_humidity_zone")
        assert(Tablas.TemperatureHumidityZoneMerge == "temperature_humidity_zone_merge")
    }

    test("Get Ruta Para Tabla") {
        val nombreTabla = "test"
        assert(getRutaParaTabla(nombreTabla) == "./tmp/test")
    }

    test("Get Ruta Para Tabla Checkpoint") {
        val nombreTabla = "test"
        assert(getRutaParaTablaChk(nombreTabla) == "./tmp/test_chk")
    }
}