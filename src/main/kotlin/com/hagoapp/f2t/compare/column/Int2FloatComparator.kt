/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.compare.CompareColumnResult
import java.sql.JDBCType
import java.sql.JDBCType.*

class Int2FloatComparator: ColumnComparator.Comparator {
    private val ranges = mapOf(
        TINYINT to 127,
        SMALLINT to 32767,
        INTEGER to Int.MAX_VALUE,
        BIGINT to Long.MAX_VALUE,
    )
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val maxLength = ranges.getValue(fileColumnDefinition.dataType).toString().length
        return if (dbColumnDefinition.dataType == DECIMAL) CompareColumnResult(
            isTypeMatched = false,
            maxLength <= dbColumnDefinition.typeModifier.precision
        )
        else CompareColumnResult(isTypeMatched = false, true)
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }
}
