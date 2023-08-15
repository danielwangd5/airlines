package com.quantexa.codeasessment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GreatestNumCountriesAnalyzerTest extends AnyFlatSpec with Matchers {
  import SharedUtils._

  // Sample flight data for testing
  val sampleFlights: List[Flight] = List(
    Flight(1, 1, "uk", "fr", "2023-08-01"),
    Flight(1, 2, "fr", "us", "2023-08-05"),
    Flight(1, 3, "us", "cn", "2023-08-10"),
    Flight(1, 4, "cn", "uk", "2023-08-15"),
    Flight(2, 5, "cn", "uk", "2023-08-01"),
    Flight(2, 6, "uk", "de", "2023-08-05"),
    Flight(2, 7, "de", "uk", "2023-08-10"),
  )


  // Tests for the findLongestRunWithoutUK function
  "processQuestion3" should "return the greatest number of countries a passenger has been in without being in the UK" in {
    val result = processQuestion3(sampleFlights)

    // Expected results based on the sample flight data
    val expectedResults = List(
      // Expected results go here
      (1, 3),
      (2, 1)
    )

    result should contain theSameElementsAs expectedResults
  }

  it should "return an empty list for an empty list of flights" in {
    val result = processQuestion3(List.empty)
    result shouldBe empty
  }
}

