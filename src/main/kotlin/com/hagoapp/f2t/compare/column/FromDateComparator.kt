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
import java.time.Instant
import java.time.format.DateTimeFormatter

class FromDateComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val formatter = if (extra.isEmpty()) DateTimeFormatter.ISO_DATE
        else DateTimeFormatter.ofPattern(extra[0])
        return when (dbColumnDefinition.dataType) {
            // DATE -> CompareColumnResult(isTypeMatched = true, true) not going to happen
            TIMESTAMP, TIMESTAMP_WITH_TIMEZONE, NVARCHAR, NCLOB -> CompareColumnResult(
                isTypeMatched = false, true
            )
            CHAR, VARCHAR, CLOB, NCHAR -> CompareColumnResult(
                isTypeMatched = false,
                formatter.format(Instant.now()).length <= dbColumnDefinition.typeModifier.maxLength
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
            FLOAT, DOUBLE, DECIMAL,
            TIMESTAMP_WITH_TIMEZONE, TIME, TIMESTAMP
        )
    }
}