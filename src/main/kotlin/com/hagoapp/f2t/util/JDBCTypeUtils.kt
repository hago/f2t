/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import com.hagoapp.util.EncodingUtils
import java.sql.JDBCType
import java.sql.JDBCType.*

class JDBCTypeUtils {
    companion object {

        fun combinePossibleTypes(a: Set<JDBCType>, b: Set<JDBCType>): Set<JDBCType> {
            return when {
                a.isEmpty() -> b
                b.isEmpty() -> a
                else -> {
                    val l = a.intersect(b)
                    l.ifEmpty { setOf(NCLOB) }
                }
            }
        }

        fun toTypedValue(value: Any?, outType: JDBCType): Any? {
            return when (value) {
                null -> null
                is String -> stringToTypedValue(value, outType)
                else -> value
            }
        }

        private val JDBC_TEXT_TYPES = listOf(
            CHAR, VARCHAR, CLOB,
            NCHAR, NVARCHAR, NCLOB
        )

        private fun stringToTypedValue(value: String, outType: JDBCType): Any? {
            if (value.isBlank() && !JDBC_TEXT_TYPES.contains(outType)) {
                return null
            }
            return when (outType) {
                TINYINT -> value.toByte()
                SMALLINT -> value.toShort()
                INTEGER -> value.toInt()
                BIGINT -> value.toLong()
                DOUBLE, DECIMAL, FLOAT -> value.toDouble()
                TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> DateTimeTypeUtils.stringToDateTimeOrNull(value)!!
                BOOLEAN -> BooleanTypeUtils.toBoolean(value)
                DATE -> DateTimeTypeUtils.stringToDateOrNull(value)!!
                TIME, TIME_WITH_TIMEZONE -> DateTimeTypeUtils.stringToTimeOrNull(value)!!
                else -> value
            }
        }

        fun guessTypes(value: String?): List<JDBCType> {
            val dl = mutableListOf<JDBCType>()
            if (value == null) {
                return dl
            }
            dl.add(NCLOB)
            dl.add(NVARCHAR)
            dl.add(NCHAR)
            if (EncodingUtils.isAsciiText(value)) {
                dl.add(CLOB)
                dl.add(VARCHAR)
                dl.add(CHAR)
            }
            dl.addAll(guessIntTypes(value))
            dl.addAll(guessFloatTypes(value))
            if (BooleanTypeUtils.isPossibleBooleanValue(value)) {
                dl.add(BOOLEAN)
            }
            dl.addAll(guessDateTimeTypes(value))
            return dl
        }

        private fun guessDateTimeTypes(value: String): Set<JDBCType> {
            val ret = mutableSetOf<JDBCType>()
            if (DateTimeTypeUtils.isDateTime(value)) {
                ret.add(TIMESTAMP_WITH_TIMEZONE)
            }
            if (DateTimeTypeUtils.isDate(value)) {
                ret.add(DATE)
            }
            if (DateTimeTypeUtils.isTime(value)) {
                ret.add(TIME)
            }
            return ret
        }

        private fun guessIntTypes(value: String): Set<JDBCType> {
            return guessIntTypes(value.toLongOrNull())
        }

        fun guessIntTypes(l: Long?): Set<JDBCType> {
            val ret = mutableSetOf<JDBCType>()
            if (l != null) {
                ret.add(BIGINT)
                if ((l <= Int.MAX_VALUE.toLong()) && (l >= Int.MIN_VALUE.toLong())) {
                    ret.add(INTEGER)
                }
                if ((l <= Short.MAX_VALUE.toLong()) && (l >= Short.MIN_VALUE.toLong())) {
                    ret.add(SMALLINT)
                }
                if ((l <= Byte.MAX_VALUE.toLong()) && (l >= Byte.MIN_VALUE.toLong())) {
                    ret.add(TINYINT)
                }
            }
            return ret
        }

        private fun guessFloatTypes(value: String): Set<JDBCType> {
            val ret = mutableSetOf<JDBCType>()
            val l = value.toBigDecimalOrNull()
            if (l != null) {
                ret.add(DECIMAL)
                if ((l <= Double.MAX_VALUE.toBigDecimal()) && (l >= Double.MIN_VALUE.toBigDecimal())) {
                    ret.addAll(guessFloatTypes(l.toDouble()))
                }
            }
            return ret
        }

        fun guessFloatTypes(value: Double?): Set<JDBCType> {
            val ret = mutableSetOf<JDBCType>()
            if (value != null) {
                ret.add(DOUBLE)
                if ((value <= Float.MAX_VALUE.toDouble()) && (value >= Float.MIN_VALUE.toDouble())) {
                    ret.add(FLOAT)
                }
            }
            return ret
        }

    }

}
