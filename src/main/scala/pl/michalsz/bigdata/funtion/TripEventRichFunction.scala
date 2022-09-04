package pl.michalsz.bigdata.funtion

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import pl.michalsz.bigdata.model.{TripEvent, TripEventWithBorough}

import scala.io.Source

class TripEventRichFunction extends RichMapFunction[TripEvent, TripEventWithBorough] {

  var boroughMap: Option[Map[Int, String]] = None

  override def open(config: Configuration): Unit = {
    val boroughFile = getRuntimeContext.getDistributedCache.getFile("borough")
    val file = Source.fromFile(boroughFile)
    val map = file.getLines()
                     .filter(s => !s.startsWith("\"LocationID\""))
                     .map(_.split(","))
                     .map(array => array(0).toInt -> array(1).replace("\"", ""))
                     .toMap
    boroughMap = Some(map)
    file.close()
  }

  override def map(tripEvent: TripEvent): TripEventWithBorough = {
    TripEventWithBorough(tripEvent.startStop,
                         tripEvent.timestamp,
                         boroughMap.get.getOrElse(tripEvent.locationId, "N/A"),
                         tripEvent.passengerCount)
  }
}
