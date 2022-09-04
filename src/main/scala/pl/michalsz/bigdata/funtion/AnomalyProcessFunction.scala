package pl.michalsz.bigdata.funtion

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import pl.michalsz.bigdata.model.{Anomaly, AnomalyAccumulator}
import pl.michalsz.bigdata.utils.Utils


class AnomalyProcessFunction extends ProcessWindowFunction[AnomalyAccumulator, Anomaly, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[AnomalyAccumulator], out: Collector[Anomaly]): Unit =
    elements.foreach(anomaly => out.collect(Anomaly(anomaly.borough,
                                                    Utils.getLocalDateTime(context.window.getStart),
                                                    Utils.getLocalDateTime(context.window.getEnd),
                                                    anomaly.arrivalPassengerCount,
                                                    anomaly.departurePassengerCount,
                                                    anomaly.departurePassengerCount - anomaly.arrivalPassengerCount)))
}
