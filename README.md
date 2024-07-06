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


*Apunte:

Al cambiar a option es necesario cambiar la creacion del dataset usando la funcion validarDatosSensorTemperatureHumidity.
Si se usa la función validarDatosSensorTemperatureHumidity como devuelve un Option[TemperatureHumidityData], 
debemos usar flatMap en lugar de map para manejar este caso adecuadamente, de manera que solo los valores Some se incluyan en el Dataset.


### Segunda refactorización

Primer objetivo: Borrado de partes que no se usan.
Segundo objetivo: Mejorar case classes para definir los sensores
En las clases para definir lo sensores veo ya aplicadas buenas practicas, enumero:
1. Ya estamos utilizando case class, lo cual es bueno.
2. Ya se está utilizando Option para zoneId.
3. Interpolación de strings en lugar de concatenación: No es necesario cambiar esto ya que no hay concatenación de strings.
4. En este fragmento no es necesario aplicar Try, Option o Either

Si que es posibles mejorar la UDF: Para mejorar el código de la UDF y asegurar que no hay valores null. 
Ademas si seguimos en el codigo se ve que podemos refactorizar el los identificadores de sensores y zonas, 
ya que son finitos y podemos manejar de manera segura los potenciales valores nulos. 
Ademas tambien se ha modificado el mapeo entre sensores y zonas.

Pero esto me hace ver que ahora es necesario refactorizar el objeto DataValidations, ¿para qué? Asegurarme de que 
se utilicen las enumeraciones definidas previamente para SensorId. Esto implica lo siguiente:

1. Uso de Enumeraciones: Hemos introducido el uso de enumeraciones para SensorId dentro de las funciones de validación, 
lo que mejora la seguridad del tipo y reduce errores.
2. Modularización: Las funciones toDouble, toTimestamp y toSensorId están definidas como funciones privadas 
para modularizar el código y reutilizar la lógica de conversión.
3. Manejo Seguro de Valores Nulos: Utilizamos Option y Try para manejar conversiones que pueden fallar, 
evitando el uso de null.

