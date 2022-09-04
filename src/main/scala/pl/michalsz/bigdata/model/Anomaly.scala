package pl.michalsz.bigdata.model

import java.time.LocalDateTime

case class Anomaly(location: String,
                   startDatetime: LocalDateTime,
                   endDatetime: LocalDateTime,
                   arrivalPassengerCount: Int,
                   departurePassengerCount: Int,
                   difference: Int) {
  override def toString: String = {
    s"""
       |{
       | "location": "$location",
       | "startDate": "$startDatetime",
       | "endDate": "$endDatetime",
       | "arrivalPassengerCount": $arrivalPassengerCount,
       | "departurePassengerCount": $departurePassengerCount,
       | "difference": $difference
       |}
       |""".stripMargin
  }
}
