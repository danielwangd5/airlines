package com.quantexa.codeasessment

import java.io.PrintWriter
import SharedUtils._

object PassengersTogetherAnalyzer {

  def main(args: Array[String]): Unit = {
    // Load flight data
    val flightData = loadFlightData("data/flightData.csv")

    // Process the data for question 4
    val passengersWithMoreThan3FlightsTogether = processQuestion4(flightData)

    // Write the results to a CSV file
    writeResults(passengersWithMoreThan3FlightsTogether, "results/q4.csv")
  }



  def writeResults(results: List[(Int, Int, Int)], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("passenger1Id, passenger2Id, numberFlightsTogether")

      // Write the results
      results.foreach { case (passenger1Id, passenger2Id, count) =>
        writer.println(s"$passenger1Id, $passenger2Id, $count")
      }
    } finally {
      writer.close()
    }
  }
}

