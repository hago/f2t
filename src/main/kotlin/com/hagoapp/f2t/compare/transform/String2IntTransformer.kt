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
 * Transformer from string to integral types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class String2IntTransformer: TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        if (src !is String?) {
            throw F2TException("Not String input to transform to int")
        }
        return when  {
            src == null -> null
            src.isBlank() -> null
            dbColumnDefinition.dataType == TINYINT -> src.toByte()
            dbColumnDefinition.dataType == SMALLINT -> src.toShort()
            dbColumnDefinition.dataType == INTEGER -> src.toInt()
            dbColumnDefinition.dataType == BIGINT -> src.toLong()
            else -> null
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }
}
