/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.util

class NumericUtils {
    companion object {
        fun detectPrecision(input: Any): Pair<Int, Int> {
            val s = input.toString()
            return try {
                s.toDouble()
                val ss = s.split("\\.").toTypedArray()
                if (ss.size != 2) {
                    Pair(0, 0)
                } else {
                    Pair(ss[0].length, ss[1].length)
                }
            } catch (ignored: NumberFormatException) {
                Pair(0, 0)
            }
        }
    }
}
