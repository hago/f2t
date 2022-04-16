/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition

/**
 * Compare result of a data file definition and a database table definition.
 *
 * @property missingColumns column names that are required by database table but not found from file
 * @property superfluousColumns column names that are provided in file but not in table schema
 * @property typeConflictedColumns  column pairs that are identical in both sides but with different definition
 * @property dataMayTruncateColumns column pairs that are compatible in type definition but with different value ranges,
 * which may cause data loss during data copying
 * @author Chaojun Sun
 * @since 0.6
 */
data class CompareTableResult(
    val missingColumns: Set<String>,
    val superfluousColumns: Set<String>,
    val typeConflictedColumns: Map<FileColumnDefinition, ColumnDefinition>,
    val dataMayTruncateColumns: Map<FileColumnDefinition, ColumnDefinition>
) {
    /**
     * Whether file and table are defined in same schema.
     *
     * @return true if same, otherwise false
     */
    fun isOfSameSchema(): Boolean {
        return missingColumns.isEmpty() && superfluousColumns.isEmpty() && typeConflictedColumns.isEmpty()
    }

    /**
     * Whether file and table are defined in identical schema, means all types are compatible and column from table
     * can cover one from file in value ranges.
     *
     * @return true if identical, otherwise false
     */
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
