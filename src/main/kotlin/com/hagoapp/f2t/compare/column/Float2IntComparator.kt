/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import com.hagoapp.f2t.compare.TypedColumnComparator
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Compare float types with int types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class Float2IntComparator : TypedColumnComparator {
    private val ranges = mapOf(
        TINYINT to Pair(127L, -128L),
        SMALLINT to Pair(32767L, -32768L),
        INTEGER to Pair(Int.MAX_VALUE.toLong(), Int.MIN_VALUE.toLong()),
        BIGINT to Pair(Long.MAX_VALUE, Long.MIN_VALUE)
    )

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val limitPair = ranges.getValue(dbColumnDefinition.dataType)
        val lengthPair = Pair(limitPair.first.toString().length, limitPair.second.toString().length)
        return CompareColumnResult(
            isTypeMatched = false,
            fileColumnDefinition.typeModifier.scale == 0 &&
                    fileColumnDefinition.typeModifier.precision <= lengthPair.first
        )
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }
}
