package pl.michalsz.bigdata.funtion

import org.apache.flink.api.common.functions.AggregateFunction
import pl.michalsz.bigdata.model.{TaxiTraffic, TripEventWithBorough}
import pl.michalsz.bigdata.utils.Utils

class TripEventAggregateFunction extends AggregateFunction[TripEventWithBorough, TaxiTraffic, TaxiTraffic] {

  override def createAccumulator(): TaxiTraffic = TaxiTraffic(null, "", 0, 0, 0, 0)

  override def add(value: TripEventWithBorough, accumulator: TaxiTraffic): TaxiTraffic = {
    if (value.startStop == 0)
      TaxiTraffic(Utils.getLocalDate(value.timestamp),
                  value.borough,
                  accumulator.arrivalCount + 1,
                  accumulator.departureCount,
                  accumulator.arrivalPassengerCount + value.passengerCount,
                  accumulator.departurePassengerCount)
    else
      TaxiTraffic(Utils.getLocalDate(value.timestamp),
                  value.borough,
                  accumulator.arrivalCount,
                  accumulator.departureCount + 1,
                  accumulator.arrivalPassengerCount,
                  accumulator.departurePassengerCount + value.passengerCount)
  }

  override def merge(acc1: TaxiTraffic, acc2: TaxiTraffic): TaxiTraffic =
    TaxiTraffic(acc1.date,
                acc1.borough,
                acc1.arrivalCount + acc2.arrivalCount,
                acc1.departureCount + acc2.departureCount,
                acc1.arrivalPassengerCount + acc2.arrivalPassengerCount,
                acc1.departurePassengerCount + acc2.departurePassengerCount)

  override def getResult(accumulator: TaxiTraffic): TaxiTraffic = accumulator

}
