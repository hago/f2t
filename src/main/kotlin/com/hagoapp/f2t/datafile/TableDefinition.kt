/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

class TableDefinition(var columns: Set<ColumnDefinition>) {
    fun match(other: TableDefinition):Boolean {
        return match(other.columns)
    }

    fun match(columns: Set<ColumnDefinition>): Boolean {
        return this.columns == columns
    }

    override fun toString(): String {
        return "TableDefinition(columns=$columns)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as TableDefinition
        return columns == other.columns
    }

    override fun hashCode(): Int {
        return columns.hashCode()
    }

}
