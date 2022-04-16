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

/**
 * Compare time with time.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class TimeComparator : TypedColumnComparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        return CompareColumnResult(isTypeMatched = true, true)
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TIME, TIME_WITH_TIMEZONE)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TIME, TIME_WITH_TIMEZONE)
    }
}
