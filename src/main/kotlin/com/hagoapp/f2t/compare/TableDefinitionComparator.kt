/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.*
import com.hagoapp.f2t.util.ColumnMatcher

class TableDefinitionComparator {
    companion object {
        fun compare(
            fileTableDefinition: TableDefinition<FileColumnDefinition>,
            dbTableDefinition: TableDefinition<ColumnDefinition>
        ): CompareTableResult {
            return compare(fileTableDefinition.columns, dbTableDefinition)
        }

        fun compare(
            fileColumnDefinitions: Set<FileColumnDefinition>,
            dbTableDefinition: TableDefinition<ColumnDefinition>
        ): CompareTableResult {
            val colMatcher = ColumnMatcher.getColumnMatcher(dbTableDefinition.caseSensitive)
            val missing = dbTableDefinition.columns.toMutableList()
            val superfluous = fileColumnDefinitions.toMutableList()
            val shared = mutableMapOf<FileColumnDefinition, ColumnDefinition>()
            val typeConflicted = mutableMapOf<FileColumnDefinition, ColumnDefinition>()
            val mayTruncate = mutableMapOf<FileColumnDefinition, ColumnDefinition>()
            var i = 0
            while (i < missing.size) {
                val col = missing[i]
                val j = superfluous.indexOfFirst { colMatcher.invoke(it.name, col.name) }
                if (j >= 0) {
                    shared[superfluous[j]] = missing[i]
                    missing.removeAt(i)
                    superfluous.removeAt(j)
                } else {
                    i++
                }
            }
            for ((fileCol, dbCol) in shared) {
                val result = ColumnComparator.compare(fileCol, dbCol)
                if (!result.isTypeMatched) {
                    typeConflicted[fileCol] = dbCol
                } else if (!result.canLoadDataFrom) {
                    mayTruncate[fileCol] = dbCol
                }
            }
            return CompareTableResult(
                missingColumns = missing.map { it.name }.toSet(),
                superfluousColumns = superfluous.map { it.name }.toSet(),
                typeConflictedColumns = typeConflicted,
                dataMayTruncateColumns = mayTruncate
            )
        }

        fun isCompliantWithConstraint(
            data: DataTable<FileColumnDefinition>,
            uniqueDefinition: TableUniqueDefinition<ColumnDefinition>
        ): UniqueConflict {
            val colMatcher = ColumnMatcher.getColumnMatcher(uniqueDefinition.caseSensitive)
            val allNeededColumns = uniqueDefinition.columns.mapNotNull { col ->
                data.columnDefinition.firstOrNull { colMatcher.invoke(it.name, col.name) }
            }
            if (allNeededColumns.size != uniqueDefinition.columns.size) {
                throw F2TException("Column definitions not match")
            }
            val colReader = data.columnDefinition.mapIndexed { i, c -> Pair(i, c) }.filter {
                allNeededColumns.any { col -> it.second.name == col.name }
            }.associate { (i, fileCol) -> Pair(fileCol.name) { row: DataRow -> row.cells[i].toString() } }
            val duplicates = mutableMapOf<String, Map<String, String>>()
            for (row in data.rows) {
                val values = allNeededColumns.associate { col ->
                    Pair(col.name, colReader.getValue(col.name).invoke(row))
                }
                val identity = buildKeyIdentity(values.map { (_, value) -> value })
                if (!duplicates.containsKey(identity)) {
                    duplicates[identity] = values
                }
            }
            return UniqueConflict(uniqueDefinition, duplicates.map { it.value })
        }

        fun isCompliantWithConstraints(
            data: DataTable<FileColumnDefinition>,
            uniqueDefinitions: List<TableUniqueDefinition<ColumnDefinition>>
        ): Pair<Boolean, List<UniqueConflict>> {
            val results = uniqueDefinitions.map { isCompliantWithConstraint(data, it) }
            return Pair(results.all { it.duplicateValues.isEmpty() }, results)
        }

        private fun buildKeyIdentity(values: List<String>): String {
            return values.joinToString("_______")
        }
    }
}
