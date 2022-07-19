/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

/**
 * This class represents definition of a table.
 *
 * @param T type of column definition
 * @property columns    columns to form this table, ordered
 * @property caseSensitive  whether this table is case-sensitive, names and values
 * @property primaryKey the primary key constraint, if any
 * @constructor create a table definition for database table or file data table
 * @author Chaojun Sun
 * @since 0.1
 */
class TableDefinition<T : ColumnDefinition>(
    var columns: List<T>,
    var caseSensitive: Boolean = true,
    var primaryKey: TableUniqueDefinition<T>? = null
) {

    /**
     * unique constraints on this table
     */
    var uniqueConstraints: Set<TableUniqueDefinition<T>> = mutableSetOf()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TableDefinition<*>

        if (columns != other.columns) return false
        if (caseSensitive != other.caseSensitive) return false
        if (primaryKey != other.primaryKey) return false
        if (uniqueConstraints != other.uniqueConstraints) return false

        return true
    }

    override fun hashCode(): Int {
        var result = columns.hashCode()
        result = 31 * result + caseSensitive.hashCode()
        result = 31 * result + (primaryKey?.hashCode() ?: 0)
        result = 31 * result + uniqueConstraints.hashCode()
        return result
    }

    override fun toString(): String {
        return """
            TableDefinition(
            columns=$columns, 
            caseSensitive=$caseSensitive, 
            primaryKey=$primaryKey, 
            uniqueConstraints=$uniqueConstraints)
            """
    }

}
