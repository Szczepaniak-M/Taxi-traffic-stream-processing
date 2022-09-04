package pl.michalsz.bigdata.utils

import pl.michalsz.bigdata.model.{TripEvent, TripEventWithBorough}

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Date, TimeZone}

object Utils {

  def getTimestamp(s: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    format.parse(s).getTime
  }

  def getLocalDate(s: String): LocalDate = {
    LocalDate.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
  }

  def getLocalDateTime(milliseconds : Long): LocalDateTime = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(milliseconds), TimeZone.getDefault.toZoneId)
  }

  def getTimestampExtended[T](t: T): Long = {
    val timeStr = t match {
      case te: TripEvent => te.timestamp
      case te: TripEventWithBorough => te.timestamp
    }
    getTimestamp(timeStr)
  }
}
