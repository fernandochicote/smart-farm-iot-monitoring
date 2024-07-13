package kafkagen

import config.Config._
import org.apache.kafka.clients.producer._

import java.sql.Timestamp
import java.util.Properties

import scala.util.{Try, Failure, Success}


object KafkaDataGenerator {

  private val props: Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", kafkaBootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }

  private val producer = new KafkaProducer[String, String](props)

  def sendData(topic: String, sensorId: String, value: Double, timestamp: Timestamp): Try[Unit] = {
    val message = topic match {
      case "temperature_humidity" => s"$sensorId,$value,$value,$timestamp"
      case "co2" => s"$sensorId,$value,$timestamp"
      case "soil_moisture" => s"$sensorId,$value,$timestamp"
      case _ => return Failure(new IllegalArgumentException("Invalid topic"))
    }
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending data to topic $topic")
    Try(producer.send(record))
  }

  def generateAndSendData(topic: String, sensorId: String): Unit = {
    val timestamp = new Timestamp(System.currentTimeMillis())
    val value = Math.random() * 100
    sendData(topic, sensorId, value, timestamp) match {
      case Success(_) => println(s"Data sent successfully to $topic")
      case Failure(ex) => println(s"Failed to send data to $topic: ${ex.getMessage}")
    }
  }

  def main(args: Array[String]): Unit = {
    val topics = List("temperature_humidity", "co2", "soil_moisture")
    val unknownSensorInterval = 50
    val delayBetweenMessages = 500

    for {
      j <- 1 to 30000
      topic <- topics
      i <- 1 to 9
    } {
      val sensorId = if (j % unknownSensorInterval == 0 && i == 3) s"sensor-chungo-$i" else s"sensor$i"
      generateAndSendData(topic, sensorId)
      Thread.sleep(delayBetweenMessages)
    }
    producer.close()
  }
}
