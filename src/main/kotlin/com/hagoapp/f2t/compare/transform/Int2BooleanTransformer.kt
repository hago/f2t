/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.TypedColumnTransformer
import java.sql.JDBCType
import java.sql.JDBCType.*

class Int2BooleanTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        if ((src !is Byte?) && (src !is Short?) && (src !is Int?) && (src !is Long?)) {
            throw Exception("Not integer input to transform to integer")
        }
        src as Number?
        return if (src == null) null else src.toLong() > 0
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(BOOLEAN)
    }
}
