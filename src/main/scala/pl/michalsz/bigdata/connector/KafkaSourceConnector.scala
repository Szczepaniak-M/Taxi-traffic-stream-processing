package pl.michalsz.bigdata.connector

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

object KafkaSourceConnector {
  def getTripDataStream(senv: StreamExecutionEnvironment, properties: ParameterTool): DataStream[String] = {
    val source = KafkaSource.builder[String]
                            .setBootstrapServers(properties.getRequired("source.kafka.bootstrap-servers"))
                            .setTopics(properties.getRequired("source.kafka.topic"))
                            .setGroupId("FlinkTaxiTraffic")
                            .setStartingOffsets(OffsetsInitializer.earliest)
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build

    senv.fromSource(source, WatermarkStrategy.noWatermarks[String], "Kafka Source")
  }
}
