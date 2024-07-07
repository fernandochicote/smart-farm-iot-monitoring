# Proyecto de Refactorización en Scala

## Descripción

Este proyecto tiene como objetivo refactorizar una base de código existente en Scala para mejorar su mantenibilidad, legibilidad y eficiencia. La refactorización incluye la reorganización del código, la mejora de la estructura de datos, la optimización de algoritmos y la implementación de mejores prácticas de programación en Scala.

## Tabla de Contenidos

- [Primera Refactorización: Funciones del Objeto DataValidations](#primera-refactorización-funciones-del-objeto-datavalidations)
- [Segunda Refactorización](#segunda-refactorización)
- [Tercera Refactorización: Script KafkaDataGenerator](#tercera-refactorización-script-kafkadatagenerator)
- [Cuarta Refactorización: Kafka y Spark](#cuarta-refactorización-kafka-y-spark)
- [Anexo: Tests](#anexo-tests)

## Primera Refactorización: Funciones del Objeto DataValidations

### Objetivo

Evitar el uso de `null`, usar `Option`, `Try` o `Either` para manejar posibles errores y modularizar el código en funciones más pequeñas y puras.

### Cambios Realizados

1. **validarDatosSensorTemperatureHumidity**:
   - Toma un `String` y un `Timestamp` como entrada.
   - Divide el `String` en partes y utiliza las funciones auxiliares `toDouble` para convertir las partes adecuadas a `Double`.
   - Si la conversión es exitosa, devuelve un `Option[TemperatureHumidityData]`; si no, devuelve `None`.

2. **validarDatosSensorTemperatureHumiditySoilMoisture**:
   - Similar a la anterior, pero para datos de humedad del suelo.
   - Utiliza `toDouble` para convertir la humedad a `Double` y `toTimestamp` para convertir el tiempo a `Timestamp`.
   - Devuelve un `Option[SoilMoistureData]` en caso de éxito y `None` en caso de fallo.

3. **validarDatosSensorCO2**:
   - Similar a las anteriores, pero para datos de CO2.
   - Utiliza `toDouble` para convertir el CO2 a `Double` y `toTimestamp` para convertir el tiempo a `Timestamp`.
   - Devuelve un `Option[CO2Data]` en caso de éxito y `None` en caso de fallo.

### Funciones Auxiliares

- **toDouble**:
   - Toma un `String` y trata de convertirlo a `Double`.
   - Devuelve `Some(Double)` si la conversión es exitosa y `None` si falla.

- **toTimestamp**:
   - Toma un `String` y trata de convertirlo a `Timestamp` usando `Timestamp.valueOf`.
   - Devuelve `Some(Timestamp)` si la conversión es exitosa y `None` si falla.

### Apunte

Al cambiar a `Option`, es necesario cambiar la creación del dataset usando la función `validarDatosSensorTemperatureHumidity`. Debemos usar `flatMap` en lugar de `map` para manejar este caso adecuadamente, de manera que solo los valores `Some` se incluyan en el Dataset.

## Segunda Refactorización

### Objetivos

1. Borrado de partes que no se usan.
2. Mejorar `case classes` para definir los sensores.

### Cambios Realizados

1. **Buenas prácticas aplicadas**:
   - Uso de `case class`.
   - Uso de `Option` para `zoneId`.
   - Uso de interpolación de strings.

2. **Mejoras en la UDF**:
   - Encapsulación de identificadores de sensores y zonas.
   - Uso de enumeraciones para `SensorId`.
   - Modularización con funciones privadas `toDouble`, `toTimestamp` y `toSensorId`.
   - Manejo seguro de valores nulos utilizando `Option` y `Try`.

## Tercera Refactorización: Script KafkaDataGenerator

### Objetivos

1. Encapsulación de `props` y `producer` con `private val`.
2. Manejo de excepciones con `Try`.
3. Mejora de la función `message` para retornar `Failure` con una excepción específica.
4. Modificación de la función `generateAndSendData`:
   - Uso de `match` para capturar `Success` y `Failure`.
5. Mejora del `main`:
   - Reemplazo de números mágicos por constantes descriptivas.
   - Uso de sintaxis `for` comprehensiva.
   - Asignación clara de `'sensor_id'`.

## Cuarta Refactorización: Kafka y Spark

### Objetivo

Refactorizar el módulo de Kafka y ampliar las funcionalidades en Spark para todos los sensores.

### Cambios Realizados

1. **Kafka**:
   - No se ha avanzado significativamente con el módulo de Kafka, por lo que no se han implementado mejoras en esta parte.

2. **Spark**:
   - Introducción de variables de configuración de la sesión dentro del fichero `Config.scala`.
   - Elevación del `setLogLevel` a `FATAL` para disminuir la traza en la consola.
   - Ordenamiento del código y cambio de la API de `Dataframes` a `Datasets`.
   - Almacenamiento de la información de todos los sensores, no solo de temperatura y humedad.
   - Expansión de la parte de `Streaming` para crear tablas y almacenar datos de todos los sensores.
   - Adición de nuevas rutas en el archivo de configuración para el resto de sensores.

### Funciones

1. **processSensorData**:
   - Filtra y agrega datos en tiempo real basándose en el campo del sensor especificado.

2. **writeToConsole**:
   - Escribe los resultados en la consola con el modo de salida y la configuración de trigger especificados y devuelve una `StreamingQuery`.

3. **awaitTermination**:
   - Espera la finalización de una lista de consultas de streaming.

## Anexo: Tests
No estoy modificando ningun test, yo por lo menos no vi ningun video explicativo dedicado (puede que fuese opcional)
y sigo sin entender su funcionamiento. Me da Problemas al validar los sensores ya que ahora el SensorId es una
enumeracion y la salida un option.

Por ejemplo seria recomendable implementar tests unitarios para las funciones `processSensorData`, `writeToConsole` y `awaitTermination` 
para garantizar el correcto funcionamiento del código y evitar errores. Implementar estos tests ayudará a verificar que todas las refactorizaciones y mejoras funcionen correctamente.











