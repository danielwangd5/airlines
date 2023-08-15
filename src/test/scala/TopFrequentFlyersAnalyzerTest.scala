package com.quantexa.codeasessment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TopFrequentFlyersAnalyzerTest extends AnyFlatSpec with Matchers {
  import SharedUtils._

  // Sample flight and passenger data for testing
  val sampleFlights: List[Flight] = List(
    Flight(1, 1, "uk", "fr", "2023-08-01"),
    Flight(2, 2, "uk", "us", "2023-08-02"),
    Flight(3, 3, "uk", "fr", "2023-08-03"),
    Flight(3, 4, "uk", "de", "2023-08-04"),
    Flight(3, 5, "uk", "ch", "2023-08-05"),
    Flight(2, 6, "ch", "fr", "2023-08-06"),
    Flight(3, 7, "uk", "fr", "2023-08-06")
  )

  // Sample passenger data as a map (passengerId -> Passenger)
  val samplePassengers: Map[Int, Passenger] = Map(
    1 -> Passenger(1, "John", "Doe"),
    2 -> Passenger(2, "Jane", "Smith"),
    3 -> Passenger(3, "Michael", "Johnson"),
    4 -> Passenger(4, "Daniel", "Wang"),
    5 -> Passenger(5, "Joe", "Thomson")
  )

  // Tests for the processQuestion2 function
  "processQuestion2" should "return the names of the 100 most frequent flyers" in {
    val result = processQuestion2(sampleFlights, samplePassengers)

    // Expected results based on the sample flight and passenger data
    val expectedResults = List(
      // Expected results go here
      (3, 4, "Michael", "Johnson"),
      (2, 2, "Jane", "Smith"),
      (1, 1, "John", "Doe")
    )

    result should contain theSameElementsAs expectedResults
  }

  it should "return an empty list for an empty list of flights" in {
    val result = processQuestion2(List.empty, samplePassengers)
    result shouldBe empty
  }

  it should "return an empty list for an empty list of passengers" in {
    val result = processQuestion2(sampleFlights, Map.empty)
    result shouldBe empty
  }

  it should "return an empty list for empty lists of flights and passengers" in {
    val result = processQuestion2(List.empty, Map.empty)
    result shouldBe empty
  }
}

