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
 * Compare string with string.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class String2StringComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return when  {
            dbColumnDefinition.dataType == CLOB || dbColumnDefinition.dataType == NCLOB -> CompareColumnResult(
                isTypeMatched = true,
                true
            )
            fileColumnDefinition.dataType == CLOB || fileColumnDefinition.dataType == NCLOB -> CompareColumnResult(
                isTypeMatched = true,
                false
            )
            else -> CompareColumnResult(
                true,
                fileColumnDefinition.typeModifier.maxLength <= dbColumnDefinition.typeModifier.maxLength
            )
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, NCHAR, NVARCHAR, CLOB, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, NCHAR, NVARCHAR, CLOB, NCLOB)
    }
}