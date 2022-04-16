/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import com.hagoapp.f2t.compare.TypedColumnComparator
import com.hagoapp.util.NumericUtils
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Compare string with integral types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class String2IntComparator : TypedColumnComparator {

    companion object {
        private val intRanges = mapOf(
            TINYINT to Pair(127L, -128L),
            SMALLINT to Pair(32767L, -32768L),
            INTEGER to Pair(Int.MAX_VALUE.toLong(), Int.MIN_VALUE.toLong()),
            BIGINT to Pair(Long.MAX_VALUE, Long.MIN_VALUE)
        )
    }

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return CompareColumnResult(isTypeMatched = false, isInRange(fileColumnDefinition, dbColumnDefinition.dataType))
    }

    private fun isInRange(fileColumnDefinition: FileColumnDefinition, dbJDBCType: JDBCType): Boolean {
        return when {
            fileColumnDefinition.minimum == null && fileColumnDefinition.maximum == null -> false
            fileColumnDefinition.maximum == null -> isGreaterThanLowerLimit(
                fileColumnDefinition,
                intRanges.getValue(dbJDBCType).second
            )
            fileColumnDefinition.minimum == null -> isLessThanUpperLimit(
                fileColumnDefinition,
                intRanges.getValue(dbJDBCType).first
            )
            else -> isGreaterThanLowerLimit(
                fileColumnDefinition,
                intRanges.getValue(dbJDBCType).second
            ) && isLessThanUpperLimit(
                fileColumnDefinition,
                intRanges.getValue(dbJDBCType).first
            )
        }
    }

    private fun isLessThanUpperLimit(fileColumnDefinition: FileColumnDefinition, limit: Long): Boolean {
        return NumericUtils.isDecimalLongValue(fileColumnDefinition.minimum) &&
                fileColumnDefinition.maximum.toLong() < limit
    }

    private fun isGreaterThanLowerLimit(fileColumnDefinition: FileColumnDefinition, limit: Long): Boolean {
        return NumericUtils.isDecimalLongValue(fileColumnDefinition.minimum) &&
                fileColumnDefinition.minimum.toLong() > limit
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

}
