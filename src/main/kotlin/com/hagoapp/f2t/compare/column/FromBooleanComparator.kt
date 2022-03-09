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

class FromBooleanComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return when (dbColumnDefinition.dataType) {
            BOOLEAN -> CompareColumnResult(isTypeMatched = true, true)
            TIMESTAMP_WITH_TIMEZONE, DATE, TIME, TIMESTAMP ->
                CompareColumnResult(isTypeMatched = false, false)
            else -> CompareColumnResult(isTypeMatched = false, true)
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(BOOLEAN)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(
            CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB,
            SMALLINT, TINYINT, INTEGER, BIGINT,
            FLOAT, DOUBLE, DECIMAL
        )
    }
}
