package pl.michalsz.bigdata

import java.sql.{Connection, DriverManager, SQLException}
import java.util.concurrent.TimeUnit

object AggregationConsumerMain {

  def main(args: Array[String]): Unit = {

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3307/taxi_traffic"
    val username = "flink_user"
    val password = "password"

    var connection: Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()
      while (true) {
        val resultSet = statement.executeQuery("SELECT * FROM taxi_traffic ORDER BY date DESC LIMIT 10")
        while (resultSet.next()) {
          val date = resultSet.getString("date")
          val borough = resultSet.getString("borough")
          val arrivalCount = resultSet.getString("arrival_count")
          val departureCount = resultSet.getString("departure_count")
          val arrivalPassengerCount = resultSet.getString("arrival_passenger_count")
          val departurePassengerCount = resultSet.getString("departure_passenger_count")
          println(f" $date%-10s | $borough%-20s | $arrivalCount%-10s | $departureCount%-10s | $arrivalPassengerCount%-10s | $departurePassengerCount%-10s")
        }
        TimeUnit.SECONDS.sleep(60)
      }
    } catch {
      case e: SQLException => e.printStackTrace()
    }
    connection.close()
  }
}
