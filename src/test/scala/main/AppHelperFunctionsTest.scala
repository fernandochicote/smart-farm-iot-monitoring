package main

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AppHelperFunctionsTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Test")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("sensorIdToZoneId should return correct zone for known sensors") {
    import AppHelperFunctions.sensorIdToZoneId
    import spark.implicits._

    val data = Seq(
      ("Sensor1", "Zone1"),
      ("Sensor4", "Zone2"),
      ("Sensor7", "Zone3")
    ).toDF("sensorId", "expectedZone")

    val result = data.withColumn("zone", sensorIdToZoneId($"sensorId"))

    result.collect().foreach { row =>
      val sensorId = row.getString(0)
      val expectedZone = row.getString(1)
      val zone = row.getString(2)
      assert(zone === expectedZone, s"Expected $expectedZone for $sensorId but got $zone")
    }
  }

  test("sensorIdToZoneId should return 'unknown' for unknown sensors") {
    import AppHelperFunctions.sensorIdToZoneId
    import spark.implicits._

    val data = Seq(
      "UnknownSensor",
      "AnotherSensor"
    ).toDF("sensorId")

    val result = data.withColumn("zone", sensorIdToZoneId($"sensorId"))

    result.collect().foreach { row =>
      val sensorId = row.getString(0)
      val zone = row.getString(1)
      assert(zone === "unknown", s"Expected 'unknown' for $sensorId but got $zone")
    }
  }

  test("getKafkaStream should create a Dataset with (value, timestamp)") {
    import AppHelperFunctions.getKafkaStream

    // Simulación de las configuraciones necesarias para kafka bootstrap servers.
    // Notar que esta prueba puede necesitar un entorno de Kafka real para funcionar correctamente.
    val topic = "testTopic"

    val kafkaStream = getKafkaStream(topic)

    // Aquí se pueden agregar más verificaciones, dependiendo del entorno de Kafka y los datos que se espera recibir.
    assert(kafkaStream.isStreaming, "Expected the dataset to be streaming")

    val schema = kafkaStream.schema
    assert(schema.fields.map(_.name) === Array("value", "timestamp"))
  }
}