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
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * Compare date with any other types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class FromDateComparator : TypedColumnComparator {

    companion object {
        @JvmField
        val DEFAULT_DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ISO_DATE
    }

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val formatter = if (extra.isEmpty()) DEFAULT_DATE_FORMATTER
        else DateTimeFormatter.ofPattern(extra[0])
        return when (dbColumnDefinition.dataType) {
            CLOB, NCLOB -> CompareColumnResult(isTypeMatched = false, true)
            CHAR, VARCHAR, NCHAR, NVARCHAR -> CompareColumnResult(
                isTypeMatched = false,
                formatter.format(ZonedDateTime.now()).length <= dbColumnDefinition.typeModifier.maxLength
            )

            else -> CompareColumnResult(isTypeMatched = false, false)
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(DATE)
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
