package pl.michalsz.bigdata.model

import java.time.LocalDate

case class TaxiTraffic(date: LocalDate,
                       borough: String,
                       arrivalCount: Int,
                       departureCount: Int,
                       arrivalPassengerCount: Int,
                       departurePassengerCount: Int)
