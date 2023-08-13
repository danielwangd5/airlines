package com.quantexa.codeasessment

import java.io.PrintWriter
import SharedUtils._

object TopFrequentFlyersAnalyzer {

  def main(args: Array[String]): Unit = {
    // Load flight and passenger data
    val flightData = loadFlightData("data/flightData.csv")
    val passengerData = loadPassengerData("data/passengers.csv")

    // Process the data for question 2
    val topFrequentFlyers = processQuestion2(flightData, passengerData)

    // Write the results to a CSV file
    writeResults(topFrequentFlyers, "results/q2.csv")
  }


  def writeResults(results: List[(Int, Int, String, String)], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("passengerId, numberFlights, firstName, lastName")

      // Write the results
      results.foreach { case (passengerId, flightCount, firstName, lastName) =>
        writer.println(s"$passengerId, $flightCount, $firstName, $lastName")
      }
    } finally {
      writer.close()
    }
  }
}
