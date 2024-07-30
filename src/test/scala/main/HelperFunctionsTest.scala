package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite

class HelperFunctionsTest extends AnyFunSuite {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .appName("HelperFunctionsTest")
    .getOrCreate()

  test("getKafkaStream should return Dataset with valid schema") {
    val topic = "test_topic"
    val result = HelperFunctions.getKafkaStream(topic)

    result.printSchema()

    val expectedSchema = new StructType()
      .add("value", StringType)
      .add("timestamp", TimestampType)

    assert(result.schema.equals(expectedSchema))
  }

}