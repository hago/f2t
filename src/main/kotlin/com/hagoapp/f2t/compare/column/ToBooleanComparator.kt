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
 * Compare other with boolean.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class ToBooleanComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return when {
            fileColumnDefinition.dataType == BOOLEAN -> CompareColumnResult(isTypeMatched = true, true)
            fileColumnDefinition.possibleTypes.contains(BOOLEAN) -> CompareColumnResult(isTypeMatched = false, true)
            else -> CompareColumnResult(isTypeMatched = false, false)
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(
            BOOLEAN,
            CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB,
            SMALLINT, TINYINT, INTEGER, BIGINT,
            FLOAT, DOUBLE, DECIMAL,
            TIMESTAMP_WITH_TIMEZONE, TIMESTAMP,
            DATE, TIME, TIME_WITH_TIMEZONE
        )
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(BOOLEAN)
    }
}
