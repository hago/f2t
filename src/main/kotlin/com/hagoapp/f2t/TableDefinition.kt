/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import java.sql.JDBCType

/**
 * This class represents definition of a table.
 */
class TableDefinition(
    var columns: Set<ColumnDefinition>,
    var caseSensitive: Boolean = true,
    var primaryKey: TableUniqueDefinition? = null
) {

    var uniqueConstraints = mutableSetOf<TableUniqueDefinition>()

    fun diff(other: TableDefinition): TableDefinitionDifference {
        val ret = diff(other.columns)
        ret.hasIdenticalPrimaryKey = primaryKey?.compare(other.primaryKey)
        ret.hasIdenticalUniqueConstraints = (uniqueConstraints.size == other.uniqueConstraints.size) &&
                (uniqueConstraints.intersect(other.uniqueConstraints).size == uniqueConstraints.size)
        return ret
    }

    fun diff(otherColumns: Set<ColumnDefinition>): TableDefinitionDifference {
        val has = mutableListOf<String>()
        val missing = mutableListOf<String>()
        val typeDiffers = mutableListOf<Triple<String, JDBCType?, JDBCType?>>()
        columns.forEach { col ->
            val otherCol = otherColumns.find { it.name.equals(col.name, !caseSensitive) }
            if (otherCol == null) {
                has.add(col.name)
            } else if (col.dataType != otherCol.dataType) {
                typeDiffers.add(Triple(col.name, col.dataType, otherCol.dataType))
            }
        }
        otherColumns.forEach { col ->
            val otherCol = columns.find { it.name.equals(col.name, !caseSensitive) }
            if (otherCol == null) {
                missing.add(col.name)
            }
        }
        return TableDefinitionDifference(has, missing, typeDiffers)
    }

    override fun toString(): String {
        return "TableDefinition(columns=$columns)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as TableDefinition
        return diff(other).containsIdenticalColumns
    }

    override fun hashCode(): Int {
        return columns.sortedBy { it.name }.map {
            Pair(it.name, it.dataType)
        }.hashCode()
    }

}
