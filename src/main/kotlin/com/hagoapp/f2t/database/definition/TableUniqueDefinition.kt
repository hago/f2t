/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.database.definition

data class TableUniqueDefinition(
    val name: String,
    val columns: Set<ColumnDefinition>,
    val caseSensitive: Boolean = true
) {
    fun compare(other: TableUniqueDefinition?): Boolean {
        return (caseSensitive == other?.caseSensitive) &&
                (str() == other.str())
    }

    private fun str(): String {
        return columns.sortedBy { if (caseSensitive) it.name else it.name.lowercase() }.joinToString("_")
    }
}
