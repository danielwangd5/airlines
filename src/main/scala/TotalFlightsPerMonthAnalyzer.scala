import java.io.PrintWriter
import scala.io.Source

object TotalFlightsPerMonthAnalyzer {
  case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

  def main(args: Array[String]): Unit = {
    // Load the flight data
    val flightData = loadFlightData("data/flightData.csv")

    // Process the flight data for question 1
    val flightsPerMonth = processQuestion1(flightData)

    // Write the results to a CSV file
    writeResults(flightsPerMonth, "results/q1.csv")
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

  def writeResults(resultMap: Map[String, Int], outputPath: String): Unit = {
    val writer = new PrintWriter(outputPath)
    try {
      // Write the header
      writer.println("month,num_flights")
      resultMap.foreach { case (yearMonth, count) =>
        writer.println(s"$yearMonth,$count")
      }
    } finally {
      writer.close()
    }
  }
}
