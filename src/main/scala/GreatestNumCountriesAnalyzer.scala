package com.quantexa.codeasessment

import scala.io.Source
import java.io.PrintWriter

object GreatestNumCountriesAnalyzer {
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

  def main(args: Array[String]): Unit = {
    // Load flight data
    val flightData = loadFlightData("data/flightData.csv")

    // Process the data for question 3
    val longestRunPerPassenger = processQuestion3(flightData)

    // Write the results to a CSV file
    writeResults(longestRunPerPassenger, "results/q3.csv")
  }

  def loadFlightData(filePath: String): List[Flight] = {
    val source = Source.fromFile(filePath)
    val flightData = try {
      source.getLines().drop(1).map(parseFlight).toList
    } finally {
      source.close()
    }
    flightData
  }

  def parseFlight(line: String): Flight = {
    val fields = line.split(",")
    Flight(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4))
  }

  def processQuestion3(flightData: List[Flight]): List[(Int, Int)] = {
    val nonUKFlights = flightData.filter(_.from != "UK")

    // Group flights by passengerId and consecutive countries visited
    val longestRunPerPassenger = nonUKFlights
      .groupBy(_.passengerId)
      .mapValues { flights =>
        val consecutiveCountries = flights.foldLeft((0, 0, 0)) { case ((maxRun, currentRun, prevCountryIdx), flight) =>
          if (flight.to != "UK") {
            // Extend the current run
            val newCurrentRun = currentRun + 1
            (Math.max(maxRun, newCurrentRun), newCurrentRun, prevCountryIdx)
          } else {
            // Reset the current run
            (maxRun, 0, prevCountryIdx + currentRun + 1)
          }
        }
        consecutiveCountries._1
      }
      .toList

    longestRunPerPassenger
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

