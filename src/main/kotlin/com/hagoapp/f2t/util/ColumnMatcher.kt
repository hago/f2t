/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import com.hagoapp.f2t.ColumnDefinition

/**
 * A utility class to compare names of 2 columns. It creates 2 compare functions in advance and return and provide
 * one of them based on case-sensitiveness.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class ColumnMatcher {
    companion object {
        private val ignoreCaseMatcher = { a: String, b: String -> a.equals(b, true) }
        private val caseSensitiveMatcher = { a: String, b: String -> a.equals(b, false) }

        /**
         * Provide a column compare function based on case-sensitiveness.
         *
         * @param caseSensitive whether to compare with case sensitiveness
         * @return a lambda to compare 2 column names, <code>(String, String) -> Boolean</code>
         */
        fun getColumnMatcher(caseSensitive: Boolean): (String, String) -> Boolean {
            return if (caseSensitive) caseSensitiveMatcher else ignoreCaseMatcher
        }

        /**
         * Check whether 2 column names are identical under given case sensitiveness.
         *
         * @param column0   first input column
         * @param column1   second input column
         * @param caseSensitive case sensitiveness flag
         * @return  true if identical, otherwise false
         */
        fun isSameName(column0: ColumnDefinition, column1: ColumnDefinition, caseSensitive: Boolean): Boolean {
            return getColumnMatcher(caseSensitive).invoke(column0.name, column1.name)
        }
    }
}
