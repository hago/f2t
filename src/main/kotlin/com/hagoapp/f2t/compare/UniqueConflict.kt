/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.TableUniqueDefinition
import com.hagoapp.f2t.util.ColumnMatcher

data class UniqueConflict<T : ColumnDefinition>(val uniqueConstraint: TableUniqueDefinition<T>) {
    private val values = mutableMapOf<String, Any>()
    private val colMatcher = ColumnMatcher.getColumnMatcher(uniqueConstraint.caseSensitive)
    fun setConflictColumn(column: String, value: Any) {
        if (uniqueConstraint.columns.any { colMatcher.invoke(it.name, column) }) {
            values[column] = value
        }
    }

    fun getConflictValues(): Map<String, Any> {
        return values
    }
}
