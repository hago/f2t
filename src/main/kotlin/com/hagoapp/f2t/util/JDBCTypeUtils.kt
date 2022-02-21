/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import com.hagoapp.f2t.F2TException
import com.hagoapp.util.EncodingUtils
import java.sql.JDBCType
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

class JDBCTypeUtils {
    companion object {
        fun combinePossibleTypes(a: List<JDBCType>, b: List<JDBCType>): List<JDBCType> {
            return when {
                a.isEmpty() -> b
                b.isEmpty() -> a
                else -> {
                    val l = a.intersect(b).toList()
                    if (l.isEmpty()) listOf(JDBCType.CLOB) else l
                }
            }
        }

        fun combinePossibleTypes(a: Set<JDBCType>, b: Set<JDBCType>): Set<JDBCType> {
            return when {
                a.isEmpty() -> b
                b.isEmpty() -> a
                else -> {
                    val l = a.intersect(b)
                    l.ifEmpty { setOf(JDBCType.NCLOB) }
                }
            }
        }

        fun guessMostAccurateType(types: List<JDBCType>): JDBCType {
            return when {
                types.isEmpty() -> JDBCType.CLOB
                types.size == 1 -> types[0]
                types.contains(JDBCType.BOOLEAN) -> JDBCType.BOOLEAN
                types.contains(JDBCType.INTEGER) || types.contains(JDBCType.BIGINT) -> JDBCType.BIGINT
                types.contains(JDBCType.DOUBLE) || types.contains(JDBCType.FLOAT)
                        || types.contains(JDBCType.DECIMAL) -> JDBCType.DOUBLE
                types.contains(JDBCType.TIMESTAMP_WITH_TIMEZONE) -> JDBCType.TIMESTAMP_WITH_TIMEZONE
                else -> JDBCType.CLOB
            }
        }

        private val dateTimeFormatters: List<DateTimeFormatter> = listOf(
            DateTimeFormatter.BASIC_ISO_DATE,
            DateTimeFormatter.ISO_DATE,
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_LOCAL_TIME,
            DateTimeFormatter.ISO_OFFSET_DATE,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ORDINAL_DATE,
            DateTimeFormatter.ISO_TIME,
            DateTimeFormatter.ISO_OFFSET_TIME,
            DateTimeFormatter.ISO_WEEK_DATE,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.RFC_1123_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()),
            DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss a").withZone(ZoneId.systemDefault())
        )

        fun stringToDateTime(input: String): ZonedDateTime {
            val d = stringToDateTimeOrNull(input)
            return d ?: throw F2TException("$input can't be converted to Timestamp")
        }

        fun stringToDateTimeOrNull(input: String): ZonedDateTime? {
            var d: ZonedDateTime? = null
            for (dtFmt in dateTimeFormatters) {
                try {
                    d = ZonedDateTime.ofInstant(Instant.from(dtFmt.parse(input)), ZoneId.systemDefault())
                    break
                } catch (ex: DateTimeParseException) {
                    //
                }
            }
            return d
        }

        fun toTypedValue(value: Any?, outType: JDBCType): Any? {
            return when (value) {
                null -> null
                is String -> stringToTypedValue(
                    value,
                    outType
                )
                else -> value
            }
        }

        private fun stringToTypedValue(value: String, outType: JDBCType): Any {
            return when (outType) {
                JDBCType.INTEGER -> value.toInt()
                JDBCType.BIGINT -> value.toLong()
                JDBCType.DOUBLE, JDBCType.DECIMAL, JDBCType.FLOAT -> value.toDouble()
                JDBCType.TIMESTAMP_WITH_TIMEZONE -> stringToDateTime(
                    value
                )
                JDBCType.BOOLEAN -> toBoolean(value)
                else -> value
            }
        }

        private fun <T> numberToTypedValue(value: T, outType: JDBCType): Any {
            return when (value) {
                is Int, is Long -> when (outType) {
                    JDBCType.INTEGER, JDBCType.BIGINT, JDBCType.FLOAT,
                    JDBCType.DOUBLE, JDBCType.DECIMAL -> value
                    else -> throw F2TException("$value can't be converted into numeric type")
                }
                else -> value as Any
            }
        }

        fun guessTypes(value: String?): List<JDBCType> {
            val dl = mutableListOf<JDBCType>()
            if (value == null) {
                return dl
            }
            dl.add(JDBCType.NCLOB)
            dl.add(JDBCType.NVARCHAR)
            dl.add(JDBCType.NCHAR)
            if (EncodingUtils.isAsciiText(value)) {
                dl.add(JDBCType.CLOB)
                dl.add(JDBCType.VARCHAR)
                dl.add(JDBCType.CHAR)
            }
            if (value.toIntOrNull() != null) {
                dl.add(JDBCType.INTEGER)
            }
            if (value.toLongOrNull() != null) {
                dl.add(JDBCType.BIGINT)
            }
            if (value.toFloatOrNull() != null) {
                dl.add(JDBCType.FLOAT)
            }
            if (value.toDoubleOrNull() != null) {
                dl.add(JDBCType.DOUBLE)
            }
            if (value.toBigDecimalOrNull() != null) {
                dl.add(JDBCType.DECIMAL)
            }
            if (isPossibleBooleanValue(value)) {
                dl.add(JDBCType.BOOLEAN)
            }
            if (stringToDateTimeOrNull(value) != null) {
                dl.add(JDBCType.TIMESTAMP_WITH_TIMEZONE)
            }
            return dl
        }

        private val possibleTrueValues = listOf("true", "yes", "y", "t")
        private val possibleFalseValues = listOf("false", "no", "n", "f")

        private fun isPossibleBooleanValue(value: String?): Boolean {
            val x = value?.trim()
            return when {
                x == null -> true
                possibleTrueValues.any { it.compareTo(x, true) == 0 } -> true
                possibleFalseValues.any { it.compareTo(x, true) == 0 } -> true
                else -> false
            }
        }

        private fun toBoolean(value: String): Boolean {
            val x = value.trim()
            return when {
                possibleTrueValues.any { it.compareTo(x, true) == 0 } -> true
                possibleFalseValues.any { it.compareTo(x, true) == 0 } -> false
                else -> x.toBoolean()
            }
        }

    }

}
