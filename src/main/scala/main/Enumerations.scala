package main

// Enumeraciones para sensorId
  object SensorIdEnum extends Enumeration with Serializable {
    type SensorId = Value
    val Sensor1, Sensor2, Sensor3, Sensor4, Sensor5, Sensor6, Sensor7, Sensor8, Sensor9, Unknown = Value
  }

  object ZoneIdEnum extends Enumeration with Serializable {
    type ZoneId = Value
    val Zone1, Zone2, Zone3 = Value
  }

