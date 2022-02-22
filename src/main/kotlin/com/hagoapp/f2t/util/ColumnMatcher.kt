/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import com.hagoapp.f2t.ColumnDefinition

/**
 * A utility class to compare names of 2 columns.
 *
 * @author suncjs
 */
class ColumnMatcher {
    companion object {
        private val ignoreCaseMatcher = { a: String, b: String -> a.equals(b, true) }
        private val caseSensitiveMatcher = { a: String, b: String -> a.equals(b, false) }
        fun getColumnMatcher(caseSensitive: Boolean): (String, String) -> Boolean {
            return if (caseSensitive) caseSensitiveMatcher else ignoreCaseMatcher
        }

        fun isSameName(column0: ColumnDefinition, column1: ColumnDefinition, caseSensitive: Boolean): Boolean {
            return getColumnMatcher(caseSensitive).invoke(column0.name, column1.name)
        }
    }
}
