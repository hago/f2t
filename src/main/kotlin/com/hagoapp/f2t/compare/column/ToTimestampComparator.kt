/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.compare.CompareColumnResult
import java.sql.JDBCType

class ToTimestampComparator : ColumnComparator.Comparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return when (fileColumnDefinition.dataType) {
            JDBCType.DATE, JDBCType.TIME, JDBCType.TIMESTAMP -> CompareColumnResult(isTypeMatched = false, true)
            else -> CompareColumnResult(isTypeMatched = false, false)
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(
            JDBCType.BOOLEAN,
            JDBCType.CHAR, JDBCType.VARCHAR, JDBCType.CLOB, JDBCType.NCHAR, JDBCType.NVARCHAR, JDBCType.NCLOB,
            JDBCType.SMALLINT, JDBCType.TINYINT, JDBCType.INTEGER, JDBCType.BIGINT,
            JDBCType.FLOAT, JDBCType.DOUBLE, JDBCType.DECIMAL,
            JDBCType.DATE, JDBCType.TIME, JDBCType.TIMESTAMP
        )
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(JDBCType.TIMESTAMP_WITH_TIMEZONE)
    }
}
