/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import com.hagoapp.f2t.F2TLogger
import java.time.DateTimeException
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.Temporal

class DateTimeTypeUtils {
    companion object {

        private val logger = F2TLogger.getLogger()

        private val dateTimeFormatters: List<DateTimeFormatter> = listOf(
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.RFC_1123_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()),
            DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss a").withZone(ZoneId.systemDefault())
        )

        private val dateFormatters: List<DateTimeFormatter> = listOf(
            DateTimeFormatter.BASIC_ISO_DATE,
            DateTimeFormatter.ISO_DATE,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ISO_OFFSET_DATE,
            DateTimeFormatter.ISO_ORDINAL_DATE,
            DateTimeFormatter.ISO_WEEK_DATE
        )

        private val timeFormatters: List<DateTimeFormatter> = listOf(
            DateTimeFormatter.ISO_LOCAL_TIME,
            DateTimeFormatter.ISO_TIME,
            DateTimeFormatter.ISO_OFFSET_TIME
        )

        @JvmStatic
        fun stringToDateTimeOrNull(input: String): ZonedDateTime? {
            return stringToDateTimeOrNull(input, emptySet())
        }

        @JvmStatic
        fun stringToDateTimeOrNull(input: String, extraFormats: Set<String> = emptySet()): ZonedDateTime? {
            var d: ZonedDateTime? = null
            for (dtFmt in dateTimeFormatters.plus(extraFormats.map { DateTimeFormatter.ofPattern(it) })) {
                try {
                    d = ZonedDateTime.ofInstant(Instant.from(dtFmt.parse(input)), ZoneId.systemDefault())
                    logger.debug("stringToDateTimeOrNull parsing successfully using $dtFmt for $input")
                    break
                } catch (ex: DateTimeParseException) {
                    //logger.debug("stringToDateTimeOrNull parsing error DateTimeParseException $ex using $dtFmt")
                } catch (ex: DateTimeException) {
                    //logger.debug("stringToDateTimeOrNull parsing error DateTimeException $ex using $dtFmt")
                }
            }
            return d
        }

        @JvmStatic
        fun stringToDateOrNull(input: String): LocalDate? {
            return stringToDateOrNull(input, emptySet())
        }

        @JvmStatic
        fun stringToDateOrNull(input: String, extraFormats: Set<String> = emptySet()): LocalDate? {
            var d: LocalDate? = null
            for (dtFmt in dateFormatters.plus(extraFormats.map { DateTimeFormatter.ofPattern(it) })) {
                try {
                    d = LocalDate.from(dtFmt.parse(input))
                    logger.debug("stringToDateOrNull parsing successfully using $dtFmt for $input")
                    break
                } catch (ex: DateTimeParseException) {
                    //logger.debug("stringToDateOrNull parsing error DateTimeParseException $ex using $dtFmt")
                } catch (ex: IllegalArgumentException) {
                    //logger.debug("stringToDateOrNull parsing error IllegalArgumentException $ex using $dtFmt")
                } catch (ex: DateTimeException) {
                    //logger.debug("stringToDateOrNull parsing error DateTimeException $ex using $dtFmt")
                }
            }
            return d
        }

        @JvmStatic
        fun stringToTimeOrNull(input: String): LocalTime? {
            return stringToTimeOrNull(input, emptySet())
        }

        @JvmStatic
        fun stringToTimeOrNull(input: String, extraFormats: Set<String> = emptySet()): LocalTime? {
            var d: LocalTime? = null
            for (dtFmt in timeFormatters.plus(extraFormats.map { DateTimeFormatter.ofPattern(it) })) {
                try {
                    d = LocalTime.from(dtFmt.parse(input))
                    logger.debug("stringToTimeOrNull parsing successfully using $dtFmt for $input")
                    break
                } catch (ex: DateTimeParseException) {
                    //logger.debug("stringToTimeOrNull parsing error DateTimeParseException $ex using $dtFmt")
                } catch (ex: IllegalArgumentException) {
                    //logger.debug("stringToTimeOrNull parsing error IllegalArgumentException $ex using $dtFmt")
                } catch (ex: DateTimeException) {
                    //logger.debug("stringToTimeOrNull parsing error DateTimeException $ex using $dtFmt")
                }
            }
            return d
        }

        @JvmStatic
        fun stringToTemporalOrNull(input: String): Temporal? {
            return stringToTemporalOrNull(input, emptySet())
        }

        @JvmStatic
        fun stringToTemporalOrNull(input: String, extraFormats: Set<String> = emptySet()): Temporal? {
            val dt = stringToDateTimeOrNull(input, extraFormats)
            if (dt != null) {
                return dt
            }
            val d = stringToDateTimeOrNull(input, extraFormats)
            if (d != null) {
                return d
            }
            val t = stringToDateTimeOrNull(input, extraFormats)
            if (t != null) {
                return t
            }
            return null
        }

        @JvmStatic
        fun isDateTime(input: String): Boolean {
            return stringToDateTimeOrNull(input) != null
        }

        @JvmStatic
        fun isDateTime(input: String, extraFormats: Set<String>): Boolean {
            return stringToDateTimeOrNull(input, extraFormats) != null
        }

        @JvmStatic
        fun isDate(input: String): Boolean {
            return stringToDateOrNull(input) != null
        }

        @JvmStatic
        fun isDate(input: String, extraFormats: Set<String>): Boolean {
            return stringToDateOrNull(input, extraFormats) != null
        }

        @JvmStatic
        fun isTime(input: String): Boolean {
            return stringToTimeOrNull(input) != null
        }

        @JvmStatic
        fun isTime(input: String, extraFormats: Set<String>): Boolean {
            return stringToTimeOrNull(input, extraFormats) != null
        }

        fun getDefaultDateTimeFormatter(): DateTimeFormatter {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME
        }

        fun getDefaultDateFormatter(): DateTimeFormatter {
            return DateTimeFormatter.ISO_OFFSET_DATE
        }

        fun getDefaultTimeFormatter(): DateTimeFormatter {
            return DateTimeFormatter.ISO_OFFSET_TIME
        }

        fun getDateTimeFormatter(format: String?): DateTimeFormatter {
            return try {
                format ?: DateTimeFormatter.ISO_OFFSET_DATE_TIME
                DateTimeFormatter.ofPattern(format)
            } catch (e: java.lang.IllegalArgumentException) {
                DateTimeFormatter.ISO_OFFSET_DATE_TIME
            }
        }

        fun getDateFormatter(format: String?): DateTimeFormatter {
            return try {
                format ?: DateTimeFormatter.ISO_OFFSET_DATE
                DateTimeFormatter.ofPattern(format)
            } catch (e: java.lang.IllegalArgumentException) {
                return DateTimeFormatter.ISO_OFFSET_DATE
            }
        }

        fun getDTimeFormatter(format: String?): DateTimeFormatter {
            return try {
                format ?: DateTimeFormatter.ISO_OFFSET_TIME
                DateTimeFormatter.ofPattern(format)
            } catch (e: java.lang.IllegalArgumentException) {
                return DateTimeFormatter.ISO_OFFSET_TIME
            }
        }
    }
}
