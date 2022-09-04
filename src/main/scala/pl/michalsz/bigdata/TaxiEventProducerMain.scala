package pl.michalsz.bigdata

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.io.Source


object TaxiEventProducerMain {

  val HEADER_SIZE = 1

  def main(args: Array[String]): Unit = {

    if (args.length != 4)
      throw new IllegalArgumentException("wrong number of arguments")

    val producerProperties = new Properties()
    producerProperties.put("files.location", args(0))
    producerProperties.put("topic.name", args(1))
    producerProperties.put("sleep.time", args(2))

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", args(3))
    kafkaProperties.put("acks", "all")
    kafkaProperties.put("retries", "0")
    kafkaProperties.put("batch.size", "16384")
    kafkaProperties.put("linger.ms", "1")
    kafkaProperties.put("buffer.memory", "33554432")
    kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](kafkaProperties)

    val filesCatalog = new File(producerProperties.getProperty("files.location"))
    val filesLocation = filesCatalog.listFiles()
                                    .map(_.getAbsolutePath)
                                    .sorted

    filesLocation.foreach(filePath => {
      val file = Source.fromFile(filePath)
      file.getLines()
          .drop(1)
          .foreach(line => producer.send(getProducerRecord(producerProperties, line)))
      file.close()
      TimeUnit.SECONDS.sleep(producerProperties.getProperty("sleep.time").toInt)
    })
  }

  def getProducerRecord(properties: Properties, line: String): ProducerRecord[String, String] =
    new ProducerRecord(properties.getProperty("topic.name"), String.valueOf(line.hashCode()), line)
}
