/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.TableUniqueDefinition

/**
 * Conflicts information for database unique constraints.
 *
 * @property uniqueConstraint  unique constraint defined for a table
 * @property duplicateValues   a map whose key is constraint name and value is duplicate value combination that violates it
 * @author Chaojun Sun
 * @since 0.2
 */
data class UniqueConflict(
    val uniqueConstraint: TableUniqueDefinition<ColumnDefinition>,
    val duplicateValues: List<Map<String, Any?>>
) {
    /**
     * Get violation values on a column.
     *
     * @param column column name
     * @return violation values from this column
     */
    fun getValueOfColumn(column: String): List<Any?> {
        return duplicateValues.map { it[column] }
    }

    /**
     * Whether conflict exists.
     *
     * @return true if conflicted, otherwise false
     */
    fun isConflicted(): Boolean {
        return duplicateValues.isNotEmpty()
    }
}
