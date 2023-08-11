package com.quantexa.codeasessment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FlightDataAnalysis {
  /***
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("FlightDataAnalysis")
      .master("local")
      .getOrCreate()

    // Load the flight data from the CSV file
    val flightData = spark.read
      .option("header", "true")
      .csv("data/flightData.csv") // Assuming flightData.csv is in the data folder

    // Define a function to extract the month from a date string
    val extractMonth = udf((date: String) => {
      val parts = date.split("-")
      parts(0) + "-" + parts(1) // Concatenate year and month with "-" to remove ambiguity
    })

    // Add a new column "month" to the flight data
    val flightsWithMonth = flightData.withColumn("month", extractMonth(col("date")))

    // Count flights per month
    val flightsPerMonth = flightsWithMonth.groupBy("month").count()

    // Display the results
    flightsPerMonth.show()

    // Stop the Spark session
    spark.stop()
  }
}
