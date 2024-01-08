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
import java.math.BigDecimal
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Compare string with floating types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class String2FloatComparator : TypedColumnComparator {
    companion object {
        private val floatRanges = mapOf(
            FLOAT to Pair(Float.MAX_VALUE.toBigDecimal(), Float.MIN_VALUE.toBigDecimal()),
            DOUBLE to Pair(Double.MAX_VALUE.toBigDecimal(), Double.MIN_VALUE.toBigDecimal())
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
            fileColumnDefinition.maximum == null -> isGreaterThanOrEqualsLowerLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).second
            )

            fileColumnDefinition.minimum == null -> isLessThanOrEqualsUpperLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).first
            )

            else -> isGreaterThanOrEqualsLowerLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).second
            ) && isLessThanOrEqualsUpperLimit(
                fileColumnDefinition,
                floatRanges.getValue(dbColumnDefinition.dataType).first
            )
        }
    }

    private fun isLessThanOrEqualsUpperLimit(fileColumnDefinition: FileColumnDefinition, limit: BigDecimal): Boolean {
        return fileColumnDefinition.maximum <= limit
    }

    private fun isGreaterThanOrEqualsLowerLimit(
        fileColumnDefinition: FileColumnDefinition,
        limit: BigDecimal
    ): Boolean {
        return fileColumnDefinition.minimum >= limit
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }
}
