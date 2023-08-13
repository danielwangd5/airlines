package com.quantexa.codeasessment

import scala.io.Source

object SharedUtils {
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)
  case class Passenger(passengerId: Int, firstName: String, lastName: String)

  // Load flight data
  def loadFlightData (filePath: String): List[Flight]
  =
  {
    val source = Source.fromFile(filePath)
    val flightData = try {
      source.getLines().drop(1).map(_parseFlight).toList
    } finally {
      source.close()
    }
    flightData
  }

  private def _parseFlight(line: String): Flight = {
    val fields = line.split(",")
    Flight(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4))
  }


  // Load passenger data
  def loadPassengerData(filePath: String): Map[Int, Passenger] = {
    val source = Source.fromFile(filePath)
    val passengerData = try {
      source.getLines().drop(1).map(_parsePassenger).map(p => p.passengerId -> p).toMap
    } finally {
      source.close()
    }
    passengerData
  }

  private def _parsePassenger(line: String): Passenger = {
    val fields = line.split(",")
    Passenger(fields(0).toInt, fields(1), fields(2))
  }


  def processQuestion1(flightData: List[Flight]): Map[String, Int] = {
    val flightsWithYearMonth = flightData.map(flight => {
      val yearMonth = flight.date.substring(0, 7)
      (yearMonth, 1)
    })

    val flightsPerMonth = flightsWithYearMonth
      .groupBy(_._1)
      .mapValues(_.size)
    flightsPerMonth
  }

  def processQuestion2(flightData: List[Flight], passengerData: Map[Int, Passenger]): List[(Int, Int, String, String)] = {
    // Calculate the number of flights per passenger
    val flightsPerPassenger = flightData.groupBy(_.passengerId).mapValues(_.size)

    // Join with passenger data and select the top 100 frequent flyers
    val topFrequentFlyers = flightsPerPassenger.toSeq
      .sortBy(-_._2) // Sort in descending order by flight count
      .take(100)
      .flatMap { case (passengerId, flightCount) =>
        passengerData.get(passengerId).map { passenger =>
          (passengerId, flightCount, passenger.firstName, passenger.lastName)
        }
      }
    topFrequentFlyers.toList
  }

  def processQuestion3(flightData: List[Flight]): List[(Int, Int)] = {
    val nonUKFlights = flightData.filter(_.from != "uk")

    // Group flights by passengerId and consecutive countries visited
    val longestRunPerPassenger = nonUKFlights
      .groupBy(_.passengerId)
      .mapValues { flights =>
        val consecutiveCountries = flights.foldLeft((0, 0, 0)) { case ((maxRun, currentRun, prevCountryIdx), flight) =>
          if (flight.to != "uk") {
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

  def processQuestion4(flightData: List[Flight]): List[(Int, Int, Int)] = {
    // Group flights by flightId
    val flightsPerFlightId = flightData.groupBy(_.flightId)

    // Filter flights with more than one passenger
    val flightsWithMultiplePassengers = flightsPerFlightId.filter(_._2.size > 1).values

    // Extract pairs of passenger IDs from flights with multiple passengers
    val passengerPairsWithFlightsTogether = flightsWithMultiplePassengers.flatMap { flights =>
      flights.map(_.passengerId).combinations(2).map { case List(passenger1, passenger2) =>
        if (passenger1 < passenger2) (passenger1, passenger2) else (passenger2, passenger1)
      }.toList
    }

    // Count the number of flights together for each passenger pair
    val passengerPairsWithFlightCounts = passengerPairsWithFlightsTogether.groupBy(identity).mapValues(_.size)

    // Filter passenger pairs with more than 3 flights together
    val passengersWithMoreThan3FlightsTogether = passengerPairsWithFlightCounts.collect {
      case ((passenger1Id, passenger2Id), count) if count > 3 =>
        (passenger1Id, passenger2Id, count)
    }.toList

    passengersWithMoreThan3FlightsTogether
  }

}
