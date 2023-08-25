package com.quantexa.codeasessment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TotalFlightsPerMonthAnalyzerTest extends AnyFlatSpec with Matchers {
  import SharedUtils._

  // Sample flight data for testing
  val sampleFlights: List[Flight] = List(
    Flight(1, 1, "uk", "fr", "2022-08-01"),
    Flight(2, 2, "uk", "us", "2022-08-02"),
    Flight(3, 3, "uk", "fr", "2022-08-05"),
    Flight(4, 4, "uk", "de", "2022-09-10"),
    Flight(5, 5, "uk", "fr", "2022-09-15"),
    Flight(6, 6, "uk", "us", "2022-10-20"),
    Flight(7, 7, "uk", "fr", "2022-10-25"),
    Flight(1, 1, "uk", "fr", "2023-01-25"),
    Flight(2, 2, "uk", "us", "2023-01-26")
  )

  // Tests for the processQuestion1 function
  "processQuestion1" should "return the total number of flights for each month" in {
    val result = processQuestion1(sampleFlights)

    // Expected results based on the sample flights
    val expectedResults = List(
      ("01", 2),  //2 flights in January, 2023
      ("08", 3), // 3 flights in August, 2022
      ("09", 2), // 2 flights in September, 2022
      ("10", 2)  // 2 flights in October, 2022

    )

    result should contain theSameElementsAs expectedResults
  }

  it should "return an empty map for an empty list of flights" in {
    val result = processQuestion1(List.empty)
    result shouldBe empty
  }
}
