/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.util

import java.math.BigDecimal
import kotlin.math.absoluteValue

/**
 * A set of convenient utilities for numeric values.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class NumericUtils {
    companion object {
        /**
         * Detect the precision and scale of input value, which could be string or other types.
         *
         * @param input the source input
         * @return  a pair of integer, the first one is the count of integral part and the second is for fraction part
         */
        @JvmStatic
        fun detectPrecision(input: Any): Pair<Int, Int> {
            val s = input.toString()
            return try {
                val ss = s.toDouble().absoluteValue.toString().split(".").toTypedArray()
                when (ss.size) {
                    1 -> Pair(ss[0].length, 0)
                    2 -> Pair(ss[0].length, if (s.contains(".")) ss[1].length else 0)
                    else -> Pair(0, 0)
                }
            } catch (ignored: NumberFormatException) {
                Pair(0, 0)
            }
        }

        /**
         * Check whether a numeric value in <code>BigDecimal</code> type is in valid ranges of <code>Double</code>.
         *
         * @param input numeric value in BigDecimal type
         * @return true if within valid ranges of double type, otherwise false
         */
        @JvmStatic
        fun isDecimalInDoubleRange(input: BigDecimal): Boolean {
            return input <= Double.MAX_VALUE.toBigDecimal() &&
                    input >= Double.MIN_VALUE.toBigDecimal()
        }

        /**
         * Check whether a numeric value in <code>BigDecimal</code> type is in valid ranges of <code>Float</code>.
         *
         * @param input numeric value in BigDecimal type
         * @return true if within valid ranges of float type, otherwise false
         */
        @JvmStatic
        fun isDecimalInFloatRange(input: BigDecimal): Boolean {
            return input <= Float.MAX_VALUE.toBigDecimal() &&
                    input >= Float.MIN_VALUE.toBigDecimal()
        }

        /**
         * Check whether a numeric value in <code>BigDecimal</code> type is in valid ranges of <code>Int</code>.
         *
         * @param input numeric value in BigDecimal type
         * @return true if within valid ranges of Int type, otherwise false
         */
        @JvmStatic
        fun isDecimalIntegralValue(input: BigDecimal): Boolean {
            return input == input.toInt().toBigDecimal()
        }

        /**
         * Check whether a numeric value in <code>BigDecimal</code> type is in valid ranges of <code>Long</code>.
         *
         * @param input numeric value in BigDecimal type
         * @return true if within valid ranges of BigInteger type, otherwise false
         */
        @JvmStatic
        fun isDecimalLongValue(input: BigDecimal): Boolean {
            return input == input.toLong().toBigDecimal()
        }
    }
}
