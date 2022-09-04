package pl.michalsz.bigdata.connector

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}


object KafkaDestinationConnector {

  def getDestinationSink(properties: ParameterTool): KafkaSink[String] = {
    KafkaSink.builder()
             .setBootstrapServers(properties.getRequired("destination.kafka.bootstrap-servers"))
             .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                                .setTopic(properties.getRequired("destination.kafka.topic"))
                                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                                .build())
             .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
             .build()
  }
}
