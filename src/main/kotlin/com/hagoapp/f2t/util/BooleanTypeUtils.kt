/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

class BooleanTypeUtils {
    companion object {
        private val possibleTrueValues = listOf("true", "yes", "y", "t")
        private val possibleFalseValues = listOf("false", "no", "n", "f")

        fun isPossibleBooleanValue(value: String?): Boolean {
            val x = value?.trim()
            return when {
                x == null -> true
                possibleTrueValues.any { it.compareTo(x, true) == 0 } -> true
                possibleFalseValues.any { it.compareTo(x, true) == 0 } -> true
                else -> false
            }
        }

        fun toBoolean(value: String): Boolean {
            val x = value.trim()
            return when {
                possibleTrueValues.any { it.compareTo(x, true) == 0 } -> true
                possibleFalseValues.any { it.compareTo(x, true) == 0 } -> false
                else -> x.toBoolean()
            }
        }

    }
}
