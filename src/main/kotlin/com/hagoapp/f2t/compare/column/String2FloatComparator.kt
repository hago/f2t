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
import com.hagoapp.util.NumericUtils
import java.math.BigDecimal
import java.sql.JDBCType
import java.sql.JDBCType.*

class String2FloatComparator : ColumnComparator.Comparator {
    companion object {
        private val floatRanges = mapOf(
            FLOAT to Pair(Float.MAX_VALUE.toBigDecimal(), Float.MIN_VALUE.toBigDecimal()),
            BIGINT to Pair(Double.MAX_VALUE.toBigDecimal(), Double.MIN_VALUE.toBigDecimal())
        )
    }

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return CompareColumnResult(isTypeMatched = false, isInRange(fileColumnDefinition, dbColumnDefinition))
    }

    private fun isInRange(fileColumnDefinition: FileColumnDefinition, dbColumnDefinition: ColumnDefinition): Boolean {
        return when {
            dbColumnDefinition.dataType == DECIMAL ->
                fileColumnDefinition.typeModifier.precision <= dbColumnDefinition.typeModifier.precision &&
                        fileColumnDefinition.typeModifier.scale <= dbColumnDefinition.typeModifier.scale
            fileColumnDefinition.minimum == null && fileColumnDefinition.maximum == null -> false
            fileColumnDefinition.maximum == null -> isGreaterThanLowerLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).second
            )
            fileColumnDefinition.minimum == null -> isLessThanUpperLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).first
            )
            else -> isGreaterThanLowerLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).second
            ) && isLessThanUpperLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).first
            )
        }
    }

    private fun isLessThanUpperLimit(fileColumnDefinition: FileColumnDefinition, limit: BigDecimal): Boolean {
        return NumericUtils.isDecimalLongValue(fileColumnDefinition.minimum) &&
                fileColumnDefinition.maximum < limit
    }

    private fun isGreaterThanLowerLimit(fileColumnDefinition: FileColumnDefinition, limit: BigDecimal): Boolean {
        return NumericUtils.isDecimalLongValue(fileColumnDefinition.minimum) &&
                fileColumnDefinition.minimum > limit
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }
}
