package com.quantexa.codeasessment

import java.io.PrintWriter
import SharedUtils._

object TotalFlightsPerMonthAnalyzer {


  def main(args: Array[String]): Unit = {
    // Load the flight data
    val flightData = loadFlightData("data/flightData.csv")

    // Process the flight data for question 1
    val flightsPerMonth = processQuestion1(flightData)

    // Write the results to a CSV file
    writeResults(flightsPerMonth, "results/q1.csv")
  }


  def writeResults(resultMap: List[(String, Int)], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("month,numFlights")
      resultMap.foreach { case (month, count) =>
        writer.println(s"$month,$count")
      }
    } finally {
      writer.close()
    }
  }
}
