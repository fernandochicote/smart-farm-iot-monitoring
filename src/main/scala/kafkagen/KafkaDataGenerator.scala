package kafkagen

import config.Config._
import org.apache.kafka.clients.producer._

import java.sql.Timestamp
import java.util.Properties
import scala.util.{Try, Failure, Success}

object KafkaDataGenerator {

  // Configuración de las propiedades del productor de Kafka
  private val props: Properties = {
    val properties = new Properties()
    properties.put("bootstrap.servers", kafkaBootstrapServers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }

  private val producer = new KafkaProducer[String, String](props)

  // Función para enviar datos al tópico de Kafka
  def sendData(topic: String, sensorId: String, value: Double, timestamp: Timestamp): Try[Unit] = {
    // Construir el mensaje basado en el tópico
    val message = topic match {
      case "temperature_humidity" => s"$sensorId,$value,$value,$timestamp"
      case "co2" => s"$sensorId,$value,$timestamp"
      case "soil_moisture" => s"$sensorId,$value,$timestamp"
      case _ => return Failure(new IllegalArgumentException("Invalid topic"))
    }
    val record = new ProducerRecord[String, String](topic, "key", message)
    println(s"Sending data to topic $topic: $message")
    Try(producer.send(record)).map(_ => ())
  }

  // Función para generar y enviar datos con posibilidad de duplicados
  def generateAndSendData(topic: String, sensorId: String, duplicate: Boolean): Unit = {
    val timestamp = new Timestamp(System.currentTimeMillis())
    val value = Math.random() * 100
    // Enviar el mensaje original
    sendData(topic, sensorId, value, timestamp) match {
      case Success(_) => println(s"Data sent successfully to $topic: $sensorId")
      case Failure(ex) => println(s"Failed to send data to $topic: ${ex.getMessage}")
    }
    // Enviar mensaje duplicado con retraso si es necesario
    if (duplicate) {
      Thread.sleep(6000) // Retraso de 6 segundos para datos duplicados
      sendData(topic, sensorId, value, timestamp) match {
        case Success(_) => println(s"Duplicate data sent successfully to $topic: $sensorId")
        case Failure(ex) => println(s"Failed to send duplicate data to $topic: ${ex.getMessage}")
      }
    }
  }

  // Función principal que controla el flujo de generación y envío de datos
  def main(args: Array[String]): Unit = {
    // val topics = List("temperature_humidity", "co2", "soil_moisture")
    val topics = List("temperature_humidity")


    val delayBetweenMessages = 4000

    for {
      j <- 1 to 30000
      topic <- topics
      i <- 1 to 9
    } {
      // Generar sensor ID, incluyendo "sensor-chungo" con una cierta lógica
      val sensorId = if (j % 2 == 0 & i == 3) s"sensor-chungo-$i" else s"sensor$i"
      println(s"Iteracion: j = $j, topic = $topic, i = $i")
      println(s"Generated sensorId: $sensorId")
      // Determinar aleatoriamente si se debe duplicar el dato
      val duplicate = Math.random() < 0.05 // 5% de probabilidad de duplicar el dato
      println(s"Duplicado: $duplicate")
      generateAndSendData(topic, sensorId, duplicate)
      Thread.sleep(delayBetweenMessages) // Retraso entre envíos
    }
    producer.close() // Cierra el productor de Kafka
  }
}


