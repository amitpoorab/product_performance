package spark_scala.utils

import java.math.RoundingMode
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
object StringUtils {

	def parseDateFromString(dateTimeString: String): LocalDateTime = {
		LocalDateTime.parse(dateTimeString.slice(0,19),DateTimeFormatter.ISO_LOCAL_DATE_TIME)
	}

}