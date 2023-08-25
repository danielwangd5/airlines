package com.quantexa.codeasessment

import scala.io.Source

object SharedUtils {
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)
  case class Passenger(passengerId: Int, firstName: String, lastName: String)

  /**
   * Loads flight data from a CSV file and returns a list of Flight objects.
   *
   * This method reads flight data from a CSV file located at the specified filePath.
   * The CSV file is expected to have the following format:
   * passengerId, flightId, from, to, date
   *
   * @param filePath The path to the CSV file containing flight data.
   * @return A list of Flight objects representing the loaded flight data.
   * @throws FileNotFoundException if the specified file path is invalid.
   * @throws IOException           if there's an issue reading the CSV file.
   */
  def loadFlightData (filePath: String): List[Flight] = {
    val source = Source.fromFile(filePath)
    val flightData = try {
      source.getLines().drop(1).map(_parseFlight).toList
    } finally {
      source.close()
    }
    flightData
  }

  // Internal method to parse flight information into Flight object
  private def _parseFlight(line: String): Flight = {
    val fields = line.split(",")
    Flight(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4))
  }


  /**
   * Loads passenger data from a CSV file and returns a map of passenger IDs to Passenger objects.
   *
   * This method reads passenger data from a CSV file located at the specified filePath.
   * The CSV file is expected to have the following format:
   * passengerId, firstName, lastName
   *
   * @param filePath The path to the CSV file containing passenger data.
   * @return A map where keys are passenger IDs and values are corresponding Passenger objects.
   * @throws FileNotFoundException if the specified file path is invalid.
   * @throws IOException           if there's an issue reading the CSV file.
   */
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


  /**
   * Find the total number of flights for each month.
   *
   * @param flightData The flight data to analyze.
   * @return A map of month-year to the number of flights in that month.
   */
  def processQuestion1(flightData: List[Flight]): List[(String, Int)] = {
    val flightsWithYearMonth = flightData.map(flight => {
      val month = flight.date.substring(5, 7)
      (month, 1)
    })

    val flightsPerMonth = flightsWithYearMonth
      .groupBy(_._1)
      .mapValues(_.size)
      .toList
      .sortBy { case (month, _) => month }

    flightsPerMonth
  }

  /**
   * Processes flight and passenger data to find the names of the 100 most frequent flyers.
   *
   * This method takes flight data and passenger data as inputs and calculates the 100 most frequent flyers.
   * It returns a list of tuples, where each tuple contains passenger information and the number of flights.
   *
   * @param flightData    A list of Flight objects representing flight data.
   * @param passengerData A map of passenger IDs to Passenger objects representing passenger data.
   * @return A list of tuples where each tuple contains passenger IDs, number of flights,
   *         first names, and last names of the 100 most frequent flyers.
   */
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

  /**
   * Processes flight data to find the greatest number of countries visited by a passenger without being in the UK.
   *
   * This method takes flight data as input and calculates the greatest number of countries a passenger has visited
   * consecutively without being in the UK. It returns a list of tuples containing passenger IDs and the longest run.
   *
   * @param flightData A list of Flight objects representing flight data.
   * @return A list of tuples where each tuple contains passenger IDs and the greatest number of consecutive countries
   *         visited without being in the UK.
   */
  def processQuestion3(flightData: List[Flight]): List[(Int, Int)] = {
    val flightsPerPassenger = flightData.groupBy(_.passengerId)

    val longestRunPerPassenger = flightsPerPassenger.map { case (passengerId, flights) =>
      val nonUKFlights = flights.filterNot(_.from == "uk").sortBy(_.date)

      val longestRun = nonUKFlights.foldLeft((0, Set.empty[String])) { case ((maxRun, countries), flight) =>
        val newCountries = countries + flight.to
        val currentRun = if (newCountries.size > countries.size) {
          newCountries.size
        } else {
          0
        }
        (Math.max(maxRun, currentRun), newCountries)
      }._1

      (passengerId, longestRun)
    }.toList.sortBy { case (_, longestRun) => longestRun }
      .reverse

    longestRunPerPassenger
  }


  /**
   * Processes flight data to find passengers who have been on more than 3 flights together.
   *
   * This method takes flight data as input and identifies pairs of passengers who have been on the same flights
   * together more than 3 times. It returns a list of tuples containing passenger IDs, the IDs of the other passengers,
   * and the number of flights they have taken together.
   *
   * @param flightData A list of Flight objects representing flight data.
   * @return A list of tuples where each tuple contains passenger IDs, the IDs of other passengers,
   *         and the number of flights taken together (more than 3).
   */
  def processQuestion4(flightData: List[Flight]): List[(Int, Int, Int)] = {
    // Group flights by flightId and date
    val flightsPerFlightIdAndDate = flightData.groupBy(flight => (flight.flightId, flight.date))

    // Filter flights with more than one passenger
    val flightsWithMultiplePassengers = flightsPerFlightIdAndDate.filter(_._2.size > 1).values

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
    }.toList.sortBy { case (_, _, count) => count }.reverse

    passengersWithMoreThan3FlightsTogether
  }


}
