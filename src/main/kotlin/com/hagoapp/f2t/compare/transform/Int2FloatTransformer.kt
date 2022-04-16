/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.TypedColumnTransformer
import java.math.BigDecimal
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Transformer from integral types to float types.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class Int2FloatTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        if ((src !is Float?) && (src !is Double?) && (src !is BigDecimal?)) {
            throw Exception("Not integer input to transform to float")
        }
        src as Number?
        return if (src == null) null
        else when (dbColumnDefinition.dataType) {
            FLOAT -> src.toFloat()
            DOUBLE -> src.toDouble()
            DECIMAL -> BigDecimal.valueOf(src.toDouble())
            else -> null
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }
}
