/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.F2TException
import java.sql.JDBCType

class ColumnsDefinition {
    private val columns: MutableList<ColumnDefinition> = mutableListOf()
    private fun findColumnDefinition(index: Int): ColumnDefinition? {
        return columns.stream().filter { cd: ColumnDefinition? -> cd!!.index == index }.findFirst().orElseGet { null }
    }

    private fun findColumnDefinition(name: String): ColumnDefinition? {
        return columns.find { it.name == name }
    }

    fun addColumn(columnIndex: Int, name: String?) {
        val existed = findColumnDefinition(columnIndex)
        when {
            (existed != null) && (existed.index != columnIndex) ->
                throw F2TException("column $columnIndex named '$name' existed with different index ${existed.index}")
            else -> columns.add(ColumnDefinition(columnIndex, name ?: "column-$columnIndex"))
        }
    }

    fun addType(columnIndex: Int, type: JDBCType) {
        val existed = findColumnDefinition(columnIndex) ?: throw F2TException("column $columnIndex not found")
        existed.possibleTypes.add(type)
    }

    fun addType(name: String, type: JDBCType) {
        val existed = findColumnDefinition(name) ?: throw F2TException("column $name not found")
        existed.possibleTypes.add(type)
    }

    @JvmOverloads
    fun guessTypes(determiner: DataTypeDeterminer = BasicTypeDeterminer()): List<ColumnDefinition> {
        return columns.map { column ->
            val cd = ColumnDefinition(column.index, column.name)
            cd.possibleTypes = mutableSetOf(determiner.determineTypes(column.possibleTypes))
            cd
        }
    }
}
