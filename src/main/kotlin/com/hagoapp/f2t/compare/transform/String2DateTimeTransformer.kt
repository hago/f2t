/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.util.DateTimeTypeUtils
import java.sql.JDBCType
import java.sql.JDBCType.*

class String2DateTimeTransformer : ColumnComparator.Transformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        if (src !is String?) {
            throw Exception("Not String input to transform to datetime")
        }
        return if (src == null) null
        else DateTimeTypeUtils.stringToDateTimeOrNull(src, extra.toSet())
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TIMESTAMP_WITH_TIMEZONE)
    }
}