package com.quantexa.codeasessment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GreatestNumCountriesSparkAnalyzerTest extends AnyFlatSpec with Matchers {
  import SharedUtils._

  // Sample flight data for testing
  val sampleFlights: List[Flight] = List(
    Flight(1, 1, "uk", "fr", "2023-08-01"),
    Flight(1, 2, "fr", "us", "2023-08-02"),
    Flight(1, 3, "us", "cn", "2023-08-03"),
    Flight(1, 4, "cn", "uk", "2023-08-04"),
    Flight(1, 6, "uk", "de", "2023-08-05"),
    Flight(1, 7, "de", "uk", "2023-08-06")
  )


  // Test for the calculateLongestRun function
  "calculateLongestRun" should "return the greatest number of countries a passenger has been in without being in the UK" in {
    val result = calculateLongestRun(sampleFlights)

    // Expected results based on the sample flight data
    val expectedResults = 3

    result shouldEqual expectedResults
  }

  // Test for a random passenger ID: 2068
  // For case 2068, there is no uk, so the total number is n+1=33
  "calculateLongestRun" should
    "return the greatest number of countries a passenger has been in without being in the UK with passenger id: 2068" in {
    val flights = filterAndOrderFlights (2068)
    val result = calculateLongestRun(flights)
    val expectedResults = 33
    // printFlights (flights)
    result shouldEqual expectedResults
  }

  //Test for a random passenger ID: 382
  // For case 382, there is uk, and the largest total number is 15
  "calculateLongestRun" should
    "return the greatest number of countries a passenger has been in without being in the UK with passenger id: 4827" in {
    val flights = filterAndOrderFlights(382)
    val result = calculateLongestRun(flights)
    val expectedResults = 15
    //printFlights(flights)
    result shouldEqual expectedResults
  }

  it should "return 0 for an empty list of flights" in {
    val result = calculateLongestRun(List.empty)
    result shouldBe 0
  }
}


