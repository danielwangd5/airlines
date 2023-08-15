package com.quantexa.codeasessment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PassengersTogetherAnalyzerTest extends AnyFlatSpec with Matchers {
  import SharedUtils._

  // Sample flight data for testing
  val sampleFlights: List[Flight] = List(
    Flight(1, 1, "uk", "fr", "2023-06-01"),
    Flight(1, 2, "fr", "us", "2023-07-01"),
    Flight(1, 3, "fr", "cn", "2023-07-02"),
    Flight(1, 4, "uk", "de", "2023-08-04"),
    Flight(2, 1, "uk", "fr", "2023-08-01"),
    Flight(2, 4, "fr", "de", "2023-08-04"),
    Flight(3, 2, "fr", "us", "2023-08-02"),
    Flight(3, 4, "de", "uk", "2023-08-04"),
    Flight(4, 1, "uk", "fr", "2023-06-01"),
    Flight(4, 2, "uk", "us", "2023-07-01"),
    Flight(4, 3, "fr", "cn", "2023-07-02"),
    Flight(4, 4, "fr", "cn", "2023-08-04"),
    Flight(5, 3, "fr", "cn", "2023-08-03"),
    Flight(6, 3, "uk", "cn", "2023-08-03")
  )

  "processQuestion4" should "return passenger pairs with more than 3 flights together" in {
    val result = processQuestion4(sampleFlights)

    // Expected results based on the sample flights
    val expectedResults = List(
      (1, 4, 4),  // Passenger 1 and Passenger 4 have taken 3 flight together
    )

    result should contain theSameElementsAs expectedResults
  }

  it should "not include passenger pairs with 3 or fewer flights together" in {
    val result = processQuestion4(sampleFlights)

    // Passenger pairs (2, 3) and (3, 4) have taken only 1 flight together each
    val nonExpectedResults = List(
      (2, 3, 1),
      (3, 4, 1)
    )

    nonExpectedResults.foreach { case (passenger1, passenger2, count) =>
      result should not contain ((passenger1, passenger2, count))
    }
  }

  it should "handle an empty list of flights" in {
    val result = processQuestion4(List.empty)
    result shouldBe empty
  }
}

