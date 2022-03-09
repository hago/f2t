/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition

data class CompareTableResult(
    val missingColumns: Set<String>,
    val superfluousColumns: Set<String>,
    val typeConflictedColumns: Map<FileColumnDefinition, ColumnDefinition>,
    val dataMayTruncateColumns: Map<FileColumnDefinition, ColumnDefinition>
) {
    fun isOfSameSchema(): Boolean {
        return missingColumns.isEmpty() && superfluousColumns.isEmpty() && typeConflictedColumns.isEmpty()
    }

    fun isIdentical(): Boolean {
        return isOfSameSchema() && dataMayTruncateColumns.isEmpty()
    }

    override fun toString(): String {
        return """CompareTableResult(
            missingColumns=$missingColumns, 
            superfluousColumns=$superfluousColumns, 
            typeConflictedColumns=$typeConflictedColumns, 
            dataMayTruncateColumns=$dataMayTruncateColumns
            )"""
    }
}
