/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

/**
 * Utility class to deal with boolean type.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class BooleanTypeUtils {
    companion object {
        private val possibleTrueValues = listOf("true", "yes", "y", "t")
        private val possibleFalseValues = listOf("false", "no", "n", "f")

        /**
         * Check whether the input words could be understood as positive or negative.
         *
         * @param value text
         * @return false if words is "false", "no", "n" or "f", otherwise true
         */
        @JvmStatic
        fun isPossibleBooleanValue(value: String?): Boolean {
            val x = value?.trim()
            return when {
                x == null -> true
                possibleTrueValues.any { it.compareTo(x, true) == 0 } -> true
                possibleFalseValues.any { it.compareTo(x, true) == 0 } -> true
                else -> false
            }
        }

        /**
         * Convert a string to boolean.
         *
         * @param value input text
         * @return corresponding boolean value
         */
        @JvmStatic
        fun toBoolean(value: String?): Boolean {
            val x = value?.trim()
            return when {
                x == null -> false
                possibleTrueValues.any { it.compareTo(x, true) == 0 } -> true
                possibleFalseValues.any { it.compareTo(x, true) == 0 } -> false
                else -> x.toBoolean()
            }
        }

    }
}
