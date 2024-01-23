/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t

/**
 * Definition of a table unique constraint.
 *
 * @param <T>   the type of column definition, should be a subtype if <code>ColumnDefinition</code>
 * @property name   constraint name
 * @property columns    list of columns, order matters
 * @property caseSensitive  whether this table is case-sensitive
 */
data class TableUniqueDefinition<T : ColumnDefinition>(
    val name: String,
    val columns: List<T>,
    val caseSensitive: Boolean = true
) {
    /**
     * Method to compare whether 2 table constraint definitions are identical。
     *
     * @param other another <code>TableUniqueDefinition</code> object
     * @return true if identical, otherwise false
     */
    fun compare(other: TableUniqueDefinition<T>?): Boolean {
        return (caseSensitive == other?.caseSensitive) &&
                (str() == other.str())
    }

    private fun str(): String {
        return columns.sortedBy { if (caseSensitive) it.name else it.name.lowercase() }.joinToString("_")
    }

    fun compareColumns(other: TableUniqueDefinition<out T>?): Boolean {
        other ?: return false
        if (columns.size != other.columns.size) {
            return false
        }
        return columns.indices.all { columns[it].name.compareTo(other.columns[it].name, !caseSensitive) == 0 }
    }
}
