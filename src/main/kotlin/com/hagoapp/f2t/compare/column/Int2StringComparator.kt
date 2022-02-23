/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.compare.CompareColumnResult
import java.sql.JDBCType
import java.sql.JDBCType.*

class Int2StringComparator : ColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition
    ): CompareColumnResult {
        return CompareColumnResult(
            false,
            fileColumnDefinition.typeModifier.maxLength - dbColumnDefinition.typeModifier.maxLength >= 0
        )
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, NCHAR, NVARCHAR)
    }

}