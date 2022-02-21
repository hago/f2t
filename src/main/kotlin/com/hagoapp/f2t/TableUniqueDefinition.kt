/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t

data class TableUniqueDefinition<T : ColumnDefinition>(
    val name: String,
    val columns: Set<T>,
    val caseSensitive: Boolean = true
) {
    fun compare(other: TableUniqueDefinition<T>?): Boolean {
        return (caseSensitive == other?.caseSensitive) &&
                (str() == other.str())
    }

    private fun str(): String {
        return columns.sortedBy { if (caseSensitive) it.name else it.name.lowercase() }.joinToString("_")
    }
}
