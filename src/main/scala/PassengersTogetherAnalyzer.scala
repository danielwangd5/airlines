package com.quantexa.codeasessment

import scala.io.Source
import java.io.PrintWriter

object PassengersTogetherAnalyzer {
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

  def main(args: Array[String]): Unit = {
    // Load flight data
    val flightData = loadFlightData("data/flightData.csv")

    // Process the data for question 4
    val passengersWithMoreThan3FlightsTogether = processQuestion4(flightData)

    // Write the results to a CSV file
    writeResults(passengersWithMoreThan3FlightsTogether, "results/q4.csv")
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

  def processQuestion4(flightData: List[Flight]): List[(Int, Int, Int)] = {
    // Group flights by passengerId
    val flightsPerPassenger = flightData.groupBy(_.passengerId)

    // Find pairs of passengers who have been on the same flights
    val passengerPairsWithFlightsTogether = flightsPerPassenger.values.flatMap { flights =>
      flights.combinations(2).collect {
        case Seq(flight1, flight2) if flight1.flightId != flight2.flightId && flight1.to == flight2.to =>
          // Two flights are together if they have different flight IDs but have the same destination
          (flight1.passengerId, flight2.passengerId)
      }
    }.toList

    // Count the number of flights together for each passenger pair
    val passengerPairsWithFlightCounts = passengerPairsWithFlightsTogether.groupBy(identity).mapValues(_.size)

    // Filter passenger pairs with more than 3 flights together
    val passengersWithMoreThan3FlightsTogether = passengerPairsWithFlightCounts.collect {
      case ((passenger1Id, passenger2Id), count) if count > 3 =>
        (passenger1Id, passenger2Id, count)
    }.toList

    passengersWithMoreThan3FlightsTogether
  }

  def writeResults(results: List[(Int, Int, Int)], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("passenger1_id, passenger2_id, number_of_flights_together")

      // Write the results
      results.foreach { case (passenger1Id, passenger2Id, count) =>
        writer.println(s"$passenger1Id, $passenger2Id, $count")
      }
    } finally {
      writer.close()
    }
  }
}

