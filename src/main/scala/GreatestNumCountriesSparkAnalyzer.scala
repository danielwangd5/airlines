package com.quantexa.codeasessment

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

object GreatestNumCountriesSparkAnalyzer {
  import SharedUtils._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GreatestNumCountriesSparkAnalyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("passengerId", IntegerType, true),
      StructField("flightId", IntegerType, true),
      StructField("from", StringType, true),
      StructField("to", StringType, true),
      StructField("date", StringType, true)
    ))

    val flightData: Dataset[Flight] = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("data/flightData.csv")
      .as[Flight]

    val longestRunPerPassenger = flightData
      .groupByKey(_.passengerId)
      .flatMapGroups { case (passengerId, flights) =>
        val flightsList = flights.toList.sortBy(_.date)
        val longestRun = calculateLongestRun(flightsList)
        Iterator((passengerId, longestRun))
      }

    val longestRunPerPassengerFinal = longestRunPerPassenger.select($"_1".alias("passengerId"), $"_2".alias("longestRun"))
      .orderBy($"longestRun".desc)

    longestRunPerPassengerFinal
      .coalesce(1)
      .write
      .option("header", "true") // Include a header row in the CSV
      .csv("results/q3")
    spark.stop()
  }
}
