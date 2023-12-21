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
import java.time.Instant
import java.time.format.DateTimeFormatter

/**
 * Compare timestamp with any other types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class FromTimestampComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val formatter = if (extra.isEmpty()) DateTimeFormatter.ISO_DATE_TIME
        else DateTimeFormatter.ofPattern(extra[0])
        return when (dbColumnDefinition.dataType) {
            TIMESTAMP -> CompareColumnResult(isTypeMatched = true, true)
            CLOB, NCLOB -> CompareColumnResult(isTypeMatched = false, true)
            CHAR, VARCHAR, NCHAR, NVARCHAR -> CompareColumnResult(
                isTypeMatched = false,
                formatter.format(Instant.now()).length <= dbColumnDefinition.typeModifier.maxLength
            )
            else -> CompareColumnResult(isTypeMatched = false, false)
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TIMESTAMP_WITH_TIMEZONE, TIMESTAMP)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(
            CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB,
            SMALLINT, TINYINT, INTEGER, BIGINT,
            FLOAT, DOUBLE, DECIMAL,
            TIMESTAMP
        )
    }
}
