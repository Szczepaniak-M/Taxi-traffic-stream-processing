package pl.michalsz.bigdata.funtion

import org.apache.flink.api.common.functions.AggregateFunction
import pl.michalsz.bigdata.model.{AnomalyAccumulator, TripEventWithBorough}

class AnomalyAggregateFunction extends AggregateFunction[TripEventWithBorough, AnomalyAccumulator, AnomalyAccumulator] {

  override def createAccumulator(): AnomalyAccumulator = AnomalyAccumulator("", 0, 0)

  override def add(value: TripEventWithBorough, accumulator: AnomalyAccumulator): AnomalyAccumulator =
    if (value.startStop == 0)
      AnomalyAccumulator(value.borough,
                         accumulator.arrivalPassengerCount + value.passengerCount,
                         accumulator.departurePassengerCount)
    else
      AnomalyAccumulator(value.borough,
                         accumulator.arrivalPassengerCount,
                         accumulator.departurePassengerCount + value.passengerCount)

  override def merge(acc1: AnomalyAccumulator, acc2: AnomalyAccumulator): AnomalyAccumulator =
    AnomalyAccumulator(acc1.borough,
                       acc1.arrivalPassengerCount + acc2.arrivalPassengerCount,
                       acc1.departurePassengerCount + acc2.departurePassengerCount)

  override def getResult(accumulator: AnomalyAccumulator): AnomalyAccumulator = accumulator

}
