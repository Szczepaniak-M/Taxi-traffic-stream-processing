package pl.michalsz.bigdata.model

case class TripEventWithBorough(startStop: Int,
                                timestamp: String,
                                borough: String,
                                passengerCount: Int) {
  def this(tripEvent: TripEvent, borough: Borough) {
    this(tripEvent.startStop,
         tripEvent.timestamp,
         borough.name,
         tripEvent.passengerCount
         )
  }
}
