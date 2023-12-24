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
import java.sql.JDBCType
import java.sql.JDBCType.*
import java.time.LocalTime
import java.time.format.DateTimeFormatter

/**
 * Compare time with any other types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class FromTimeComparator : TypedColumnComparator {

    companion object {
        @JvmField
        val DEFAULT_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ISO_TIME
    }

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val formatter = if (extra.isEmpty()) DEFAULT_TIME_FORMATTER
        else DateTimeFormatter.ofPattern(extra[0])
        val possibleMaxLen = formatter.format(LocalTime.of(1, 1, 1, 111111111)).length
        return when (dbColumnDefinition.dataType) {
            TIME -> CompareColumnResult(isTypeMatched = true, true)
            CLOB, NCLOB -> CompareColumnResult(isTypeMatched = false, true)
            CHAR, VARCHAR, NCHAR, NVARCHAR -> CompareColumnResult(
                false,
                (if (fileColumnDefinition.dataType == TIME) possibleMaxLen
                else possibleMaxLen + 5) <= dbColumnDefinition.typeModifier.maxLength
            )

            else -> CompareColumnResult(isTypeMatched = false, false)
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TIME, TIME_WITH_TIMEZONE)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(
            BOOLEAN,
            CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB,
            SMALLINT, TINYINT, INTEGER, BIGINT,
            FLOAT, DOUBLE, DECIMAL
        )
    }
}
