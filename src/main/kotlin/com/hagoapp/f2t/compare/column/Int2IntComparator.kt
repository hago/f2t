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
 * Compare int with int.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class Int2IntComparator : TypedColumnComparator {

    companion object {
        val ranks = mapOf(
            TINYINT to 0,
            SMALLINT to 1,
            INTEGER to 2,
            BIGINT to 3
        )
    }

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        val r0 = ranks.getValue(fileColumnDefinition.dataType)
        val r1 = ranks.getValue(dbColumnDefinition.dataType)
        return CompareColumnResult(isTypeMatched = true, r0 <= r1)
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }
}
