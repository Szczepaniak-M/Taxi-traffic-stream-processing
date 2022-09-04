package pl.michalsz.bigdata.model

case class TripEvent(startStop: Int,
                     timestamp: String,
                     locationId: Int,
                     passengerCount: Int)
