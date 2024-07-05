### Primera refactorización: Funciones del objeto DataValidations

Objetivo:  Evitar el uso de null, usar Option, Try o Either para manejar posibles errores, y modularizar el código en 
funciones más pequeñas y puras.

Refactorizo el objeto DataValidations para que use las funciones auxiliares toDouble y toTimestamp, y devuelva Option 
en caso de errores de validación. Explicacion:

- **validarDatosSensorTemperatureHumidity**: Esta función toma un String y un Timestamp como entrada, divide el String 
en partes y utiliza las funciones auxiliares toDouble para convertir las partes adecuadas a Double. Si la conversión es 
exitosa, devuelve un Option[TemperatureHumidityData] con los datos; si no, devuelve None.
- **validarDatosSensorTemperatureHumiditySoilMoisture**: Similar a la anterior, pero para datos de humedad del suelo. 
Utiliza toDouble para convertir la humedad a Double y toTimestamp para convertir el tiempo a Timestamp. 
Devuelve un Option[SoilMoistureData] en caso de éxito y None en caso de fallo.
- **validarDatosSensorCO2**: Similar a las anteriores, pero para datos de CO2. Utiliza toDouble para convertir el CO2 a 
Double y toTimestamp para convertir el tiempo a Timestamp. Devuelve un Option[CO2Data] en caso de éxito y None en 
caso de fallo.

Se han creado las siguientes funciones auxiliares:

- **toDouble**: Esta función toma un String y trata de convertirlo a Double. Devuelve Some(Double) si la conversión 
es exitosa y None si falla.
- **toTimestamp**: Esta función toma un String y trata de convertirlo a Timestamp usando Timestamp.valueOf. 
Devuelve Some(Timestamp) si la conversión es exitosa y None si falla.