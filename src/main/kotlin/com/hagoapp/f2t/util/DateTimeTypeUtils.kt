/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import org.slf4j.LoggerFactory
import java.time.DateTimeException
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.Temporal

/**
 * Utility class to deal with date / time / timestamp types and values.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
class DateTimeTypeUtils {
    companion object {

        private val logger = LoggerFactory.getLogger(DateTimeTypeUtils::class.java)

        private val dateTimeFormatters: List<DateTimeFormatter> = listOf(
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.RFC_1123_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault()),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()),
            DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss a")
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
            DateTimeFormatter.ISO_OFFSET_TIME,
            DateTimeFormatter.ofPattern("H:m:s")
        )

        /**
         * Convert a string to ZonedDateTime using built-in formatters. Null is returned if conversions
         * are failed.
         *
         * @param input source string
         * @return zoned datetime
         */
        @JvmStatic
        fun stringToDateTimeOrNull(input: String): ZonedDateTime? {
            return stringToDateTimeOrNull(input, emptySet())
        }

        /**
         * Convert a string to ZonedDateTime using built-in formatters and customized extra formatters. NUll
         * is returned if conversions are failed.
         *
         * @param input source string
         * @param extraFormats extra formatters
         * @return zoned datetime
         */
        @JvmStatic
        fun stringToDateTimeOrNull(input: String, extraFormats: Set<String> = emptySet()): ZonedDateTime? {
            var d: ZonedDateTime? = null
            for (dtFmt in dateTimeFormatters.plus(extraFormats.map { DateTimeFormatter.ofPattern(it) })) {
                try {
                    d = ZonedDateTime.ofInstant(Instant.from(dtFmt.parse(input)), ZoneId.systemDefault())
                    break
                } catch (ignore: DateTimeParseException) {
                    //
                } catch (ignore: DateTimeException) {
                    //
                }
            }
            return d
        }

        /**
         * Convert a string to LocalDate using built-in formatters. Null is returned if conversions
         * are failed.
         *
         * @param input source string
         * @return local date
         */
        @JvmStatic
        fun stringToDateOrNull(input: String): LocalDate? {
            return stringToDateOrNull(input, emptySet())
        }

        /**
         * Convert a string to LocalDate using built-in formatters and customized extra formatters. NUll
         * is returned if conversions are failed.
         *
         * @param input source string
         * @param extraFormats extra formatters
         * @return local date
         */
        @JvmStatic
        fun stringToDateOrNull(input: String, extraFormats: Set<String> = emptySet()): LocalDate? {
            var d: LocalDate? = null
            for (dtFmt in dateFormatters.plus(extraFormats.map { DateTimeFormatter.ofPattern(it) })) {
                try {
                    d = LocalDate.from(dtFmt.parse(input))
                    break
                } catch (ignored: DateTimeParseException) {
                    //
                } catch (ignored: IllegalArgumentException) {
                    //
                } catch (ignored: DateTimeException) {
                    //
                }
            }
            return d
        }

        /**
         * Convert a string to LocalTime using built-in formatters. Null is returned if conversions
         * are failed.
         *
         * @param input source string
         * @return local time
         */
        @JvmStatic
        fun stringToTimeOrNull(input: String): LocalTime? {
            return stringToTimeOrNull(input, emptySet())
        }

        /**
         * Convert a string to LocalDate using built-in formatters and customized extra formatters. NUll
         * is returned if conversions are failed.
         *
         * @param input source string
         * @param extraFormats extra formatters
         * @return local time
         */
        @JvmStatic
        fun stringToTimeOrNull(input: String, extraFormats: Set<String> = emptySet()): LocalTime? {
            var d: LocalTime? = null
            for (dtFmt in timeFormatters.plus(extraFormats.map { DateTimeFormatter.ofPattern(it) })) {
                try {
                    d = LocalTime.from(dtFmt.parse(input))
                    break
                } catch (ex: DateTimeParseException) {
                    //
                } catch (ex: IllegalArgumentException) {
                    //
                } catch (ex: DateTimeException) {
                    //
                }
            }
            return d
        }

        /**
         * Convert a string to a Temporal using built-in formatters. Null is returned if conversions
         * are failed.
         *
         * @param input source string
         * @return temporal value
         */
        @JvmStatic
        fun stringToTemporalOrNull(input: String): Temporal? {
            return stringToTemporalOrNull(input, emptySet())
        }

        /**
         * Convert a string to Temporal using built-in formatters and customized extra formatters. NUll
         * is returned if conversions are failed.
         *
         * @param input source string
         * @param extraFormats extra formatters
         * @return temporal value
         */
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

        /**
         * Check whether a string is convertible to Datetime using built-in formatters.
         *
         * @param input input string
         * @return true if convertible, otherwise false
         */
        @JvmStatic
        fun isDateTime(input: String): Boolean {
            return stringToDateTimeOrNull(input) != null
        }

        /**
         * Check whether a string is convertible to Datetime with built-in and extra formatters.
         *
         * @param input input string
         * @param extraFormats extra formatters
         * @return true if convertible, otherwise false
         */
        @JvmStatic
        fun isDateTime(input: String, extraFormats: Set<String>): Boolean {
            return stringToDateTimeOrNull(input, extraFormats) != null
        }

        /**
         * Check whether a string is convertible to Date using built-in formatters.
         *
         * @param input input string
         * @return true if convertible, otherwise false
         */
        @JvmStatic
        fun isDate(input: String): Boolean {
            return stringToDateOrNull(input) != null
        }

        /**
         * Check whether a string is convertible to date with built-in and extra formatters.
         *
         * @param input input string
         * @param extraFormats extra formatters
         * @return true if convertible, otherwise false
         */
        @JvmStatic
        fun isDate(input: String, extraFormats: Set<String>): Boolean {
            return stringToDateOrNull(input, extraFormats) != null
        }

        /**
         * Check whether a string is convertible to time using built-in formatters.
         *
         * @param input input string
         * @return true if convertible, otherwise false
         */
        @JvmStatic
        fun isTime(input: String): Boolean {
            return stringToTimeOrNull(input) != null
        }

        /**
         * Check whether a string is convertible to time with built-in and extra formatters.
         *
         * @param input input string
         * @param extraFormats extra formatters
         * @return true if convertible, otherwise false
         */
        @JvmStatic
        fun isTime(input: String, extraFormats: Set<String>): Boolean {
            return stringToTimeOrNull(input, extraFormats) != null
        }

        /**
         * Return the default datetime formatter that will be used in F2T, <code>ISO_OFFSET_DATE_TIME</code>
         * is preset.
         *
         * @return date time formatter
         */
        fun getDefaultDateTimeFormatter(): DateTimeFormatter {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME
        }

        /**
         * Return the default date formatter that will be used in F2T, <code>ISO_DATE</code>
         * is preset.
         *
         * @return date formatter
         */
        fun getDefaultDateFormatter(): DateTimeFormatter {
            return DateTimeFormatter.ISO_DATE
        }

        /**
         * Return the default time formatter that will be used in F2T, <code>ISO_OFFSET_TIME</code>
         * is preset.
         *
         * @return time formatter
         */
        fun getDefaultTimeFormatter(): DateTimeFormatter {
            return DateTimeFormatter.ISO_TIME
        }

        /**
         * Create a datetime formatter using given format string, if the string is invalid default one is returned.
         *
         * @param format format string
         * @return formatter
         */
        fun getDateTimeFormatter(format: String?): DateTimeFormatter {
            return try {
                if (format == null) getDefaultDateTimeFormatter()
                else DateTimeFormatter.ofPattern(format)
            } catch (e: java.lang.IllegalArgumentException) {
                getDefaultDateTimeFormatter()
            }
        }

        /**
         * Create a date formatter using given format string, if the string is invalid default one is returned.
         *
         * @param format format string
         * @return formatter
         */
        fun getDateFormatter(format: String?): DateTimeFormatter {
            return try {
                if (format == null) getDefaultDateFormatter()
                else DateTimeFormatter.ofPattern(format)
            } catch (e: java.lang.IllegalArgumentException) {
                getDefaultDateFormatter()
            }
        }

        /**
         * Create a time formatter using given format string, if the string is invalid default one is returned.
         *
         * @param format format string
         * @return formatter
         */
        fun getDTimeFormatter(format: String?): DateTimeFormatter {
            return try {
                if (format == null) getDefaultTimeFormatter()
                else DateTimeFormatter.ofPattern(format)
            } catch (e: java.lang.IllegalArgumentException) {
                getDefaultTimeFormatter()
            }
        }
    }
}
