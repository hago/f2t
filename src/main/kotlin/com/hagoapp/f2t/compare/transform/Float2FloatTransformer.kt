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

class Float2FloatTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        if ((src !is Float?) && (src !is Double?) && (src !is BigDecimal?)) {
            throw Exception("Not float input to transform to float")
        }
        src as Number?
        return when {
            src == null -> null
            dbColumnDefinition.dataType == FLOAT -> src.toFloat()
            dbColumnDefinition.dataType == DOUBLE -> src.toDouble()
            src is BigDecimal -> src
            else -> BigDecimal(src.toDouble())
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(FLOAT, DOUBLE, DECIMAL)
    }
}
