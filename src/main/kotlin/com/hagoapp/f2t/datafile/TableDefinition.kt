/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.F2TException
import java.sql.JDBCType

/**
 * This class represents column definitions of a table.
 */
class TableDefinition(var columns: Set<ColumnDefinition>) {

    init {
        val x = columns.filter { it.inferredType != null }
        if (x.isNotEmpty()) {
            throw F2TException("type${if (x.size > 1) "s" else ""} not inferred for: ${x.joinToString { it.name }}")
        }
    }

    fun diff(other: TableDefinition, caseSensitive:Boolean = true): TableDefinitionDifference {
        return diff(other.columns, caseSensitive)
    }

    fun diff(otherColumns: Set<ColumnDefinition>, caseSensitive:Boolean = true): TableDefinitionDifference {
        val has = mutableListOf<String>()
        val missing = mutableListOf<String>()
        val typeDiffers = mutableListOf<Triple<String, JDBCType?, JDBCType?>>()
        columns.forEach { col ->
            val otherCol = otherColumns.find { it.name.equals(col.name, !caseSensitive) }
            if (otherCol == null) {
                has.add(col.name)
            } else if (col.inferredType != otherCol.inferredType) {
                typeDiffers.add(Triple(col.name, col.inferredType, otherCol.inferredType))
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
        return diff(other).noDifference
    }

    override fun hashCode(): Int {
        return columns.sortedBy { it.name }.map {
            Pair(it.name, it.inferredType)
        }.hashCode()
    }

}
