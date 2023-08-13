package com.quantexa.codeasessment

import java.io.PrintWriter
import SharedUtils._

object GreatestNumCountriesAnalyzer {

  def main(args: Array[String]): Unit = {
    // Load flight data
    val flightData = loadFlightData("data/flightData.csv")

    // Process the data for question 3
    val longestRunPerPassenger = processQuestion3(flightData)

    // Write the results to a CSV file
    writeResults(longestRunPerPassenger, "results/q3.csv")
  }

  def writeResults(results: List[(Int, Int)], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("passengerId, longestRun")

      // Write the results
      results.foreach { case (passengerId, longestRun) =>
        writer.println(s"$passengerId, $longestRun")
      }
    } finally {
      writer.close()
    }
  }
}

