package pl.michalsz.bigdata

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.Collections.singletonList
import java.util.Properties
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object AnomalyConsumerMain {

  def main(args: Array[String]): Unit = {

    if (args.length != 1)
      throw new IllegalArgumentException("wrong number of arguments")

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", args(0))
    kafkaProperties.put("group.id", "AnomalyReader")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](kafkaProperties)
    consumer.subscribe(singletonList("taxi-traffic-anomalies"))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(15)).asScala
      records.foreach(record => println(record.value()))
    }
    consumer.close()
  }
}
