import scala.io.Source
import java.io.PrintWriter

object TopFrequentFlyersAnalyzer {
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)
  case class Passenger(passengerId: Int, firstName: String, lastName: String)

  def main(args: Array[String]): Unit = {
    // Load flight and passenger data
    val flightData = loadFlightData("data/flightData.csv")
    val passengerData = loadPassengerData("data/passengers.csv")

    // Process the data for question 2
    val topFrequentFlyers = processQuestion2(flightData, passengerData)

    // Write the results to a CSV file
    writeResults(topFrequentFlyers, "results/q2.csv")
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

  def loadPassengerData(filePath: String): Map[Int, Passenger] = {
    val source = Source.fromFile(filePath)
    val passengerData = try {
      source.getLines().drop(1).map(parsePassenger).map(p => p.passengerId -> p).toMap
    } finally {
      source.close()
    }
    passengerData
  }

  def parseFlight(line: String): Flight = {
    val fields = line.split(",")
    Flight(fields(0).toInt, fields(1).toInt, fields(2), fields(3), fields(4))
  }

  def parsePassenger(line: String): Passenger = {
    val fields = line.split(",")
    Passenger(fields(0).toInt, fields(1), fields(2))
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

  def writeResults(results: List[(Int, Int, String, String)], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("passengerId, number of flights, first name, last name")

      // Write the results
      results.foreach { case (passengerId, flightCount, firstName, lastName) =>
        writer.println(s"$passengerId, $flightCount, $firstName, $lastName")
      }
    } finally {
      writer.close()
    }
  }
}
