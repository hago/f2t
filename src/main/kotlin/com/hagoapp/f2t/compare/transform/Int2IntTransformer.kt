/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.TypedColumnTransformer
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Transformer from inters to integers.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class Int2IntTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        if (src == null) {
            return null
        }
        if ((src !is Byte) && (src !is Short) && (src !is Int) && (src !is Long)) {
            throw F2TException("Not integer input to transform to integer: ${src::class.java.canonicalName}")
        }
        src as Number?
        return when (dbColumnDefinition.dataType) {
            TINYINT -> src.toByte()
            SMALLINT -> src.toShort()
            INTEGER -> src.toInt()
            BIGINT -> src.toLong()
            else -> null
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }
}
