/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.TypedColumnTransformer
import com.hagoapp.f2t.util.DateTimeTypeUtils
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Transformer from string to data time types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class String2DateTimeTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        src ?: return null
        if (src !is String?) {
            throw Exception("Not String input to transform to datetime: ${src::class.java.canonicalName}")
        }
        return DateTimeTypeUtils.stringToDateTimeOrNull(src, extra.toSet())
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TIMESTAMP, TIMESTAMP_WITH_TIMEZONE)
    }
}
