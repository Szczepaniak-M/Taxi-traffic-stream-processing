package pl.michalsz.bigdata.connector

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import pl.michalsz.bigdata.model.TaxiTraffic

import java.sql.{Date, PreparedStatement}

object MySqlDestinationConnector {

  def getDestinationSink(properties: ParameterTool): SinkFunction[TaxiTraffic] = {

    val connection = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(properties.getRequired("destination.mysql.url"))
      .withDriverName("com.mysql.cj.jdbc.Driver")
      .withUsername(properties.getRequired("destination.mysql.user"))
      .withPassword(properties.getRequired("destination.mysql.password"))
      .build()

    val executionOptions = JdbcExecutionOptions.builder()
                                               .withBatchSize(1)
                                               .withBatchIntervalMs(200)
                                               .withMaxRetries(5)
                                               .build()

    val statementBuilder: JdbcStatementBuilder[TaxiTraffic] =
      new JdbcStatementBuilder[TaxiTraffic] {
        override def accept(ps: PreparedStatement, data: TaxiTraffic): Unit = {
          ps.setDate(1, Date.valueOf(data.date))
          ps.setString(2, data.borough)
          ps.setInt(3, data.arrivalCount)
          ps.setInt(4, data.departureCount)
          ps.setInt(5, data.arrivalPassengerCount)
          ps.setInt(6, data.departurePassengerCount)
        }
      }

    JdbcSink.sink(
      """
        |INSERT INTO taxi_traffic
        |    (date, borough, arrival_count, departure_count, arrival_passenger_count, departure_passenger_count)
        |VALUES (?, ?, ?, ?, ?, ?) AS new
        |ON DUPLICATE KEY UPDATE
        |    arrival_count = new.arrival_count,
        |    departure_count = new.departure_count,
        |    arrival_passenger_count = new.arrival_passenger_count,
        |    departure_passenger_count = new.departure_passenger_count;
        |""".stripMargin,
      statementBuilder,
      executionOptions,
      connection)
  }

}
