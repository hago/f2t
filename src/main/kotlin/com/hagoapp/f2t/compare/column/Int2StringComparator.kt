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
 * Compare int with string.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class Int2StringComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        if ((dbColumnDefinition.dataType == CLOB) || (dbColumnDefinition.dataType == NCLOB)) {
            return CompareColumnResult(isTypeMatched = false, canLoadDataFrom = true)
        }
        return CompareColumnResult(
            isTypeMatched = false,
            canLoadDataFrom = fileColumnDefinition.typeModifier.maxLength - dbColumnDefinition.typeModifier.maxLength >= 0
        )
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, NCHAR, NVARCHAR, CLOB, NCLOB)
    }

}