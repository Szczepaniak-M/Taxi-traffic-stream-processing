package pl.michalsz.bigdata

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import pl.michalsz.bigdata.connector.{KafkaDestinationConnector, KafkaSourceConnector, MySqlDestinationConnector}
import pl.michalsz.bigdata.funtion.{AnomalyAggregateFunction, AnomalyProcessFunction, TripEventAggregateFunction, TripEventRichFunction}
import pl.michalsz.bigdata.model.{TripEvent, TripEventWithBorough}
import pl.michalsz.bigdata.utils.Utils.getTimestampExtended

import java.time.Duration


object TaxiTrafficStreamMain {

  def main(args: Array[String]): Unit = {

    val propertiesFromFile = ParameterTool.fromPropertiesFile("src/main/resources/flink.properties")
    val propertiesFromArgs = ParameterTool.fromArgs(args)
    val properties = propertiesFromFile.mergeWith(propertiesFromArgs)

    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    senv.registerCachedFile(properties.getRequired("source.static-file"), "borough")

    val inputStream = KafkaSourceConnector.getTripDataStream(senv, properties)

    val tripEventsWithBoroughDS = inputStream.filter(s => !s.startsWith("tripID"))
                                             .map(_.split(","))
                                             .filter(_.length == 9)
                                             .map(array => TripEvent(array(1).toInt, array(2),
                                                                     array(3).toInt, array(4).toInt))
                                             .map(new TripEventRichFunction)
                                             .assignTimestampsAndWatermarks(
                                               WatermarkStrategy.forBoundedOutOfOrderness[TripEventWithBorough](Duration.ofDays(1))
                                                                .withTimestampAssigner(new SerializableTimestampAssigner[TripEventWithBorough] {
                                                                  override def extractTimestamp(element: TripEventWithBorough, recordTimestamp: Long): Long =
                                                                    getTimestampExtended[TripEventWithBorough](element)
                                                                }))

    val taxiTrafficDS = tripEventsWithBoroughDS.keyBy(_.borough)
                                               .window(TumblingEventTimeWindows.of(Time.hours(24)))
                                               .trigger(ContinuousEventTimeTrigger.of[TimeWindow](getTime(properties)))
                                               .aggregate(new TripEventAggregateFunction)

    taxiTrafficDS.addSink(MySqlDestinationConnector.getDestinationSink(properties))

    val anomaliesDS = tripEventsWithBoroughDS.keyBy(_.borough)
                                             .window(SlidingEventTimeWindows.of(Time.hours(properties.getRequired("anomaly-duration").toInt), Time.hours(1)))
                                             .aggregate(new AnomalyAggregateFunction, new AnomalyProcessFunction)
                                             .filter(_.difference > properties.getRequired("anomaly-size").toInt)
                                             .map(_.toString)

    anomaliesDS.sinkTo(KafkaDestinationConnector.getDestinationSink(properties))

    senv.execute("Analyse taxi traffic and anomalies")
  }

  def getTime(properties: ParameterTool): Time =
    if (properties.getRequired("history").equals("true")) Time.hours(1) else Time.seconds(10)

}



