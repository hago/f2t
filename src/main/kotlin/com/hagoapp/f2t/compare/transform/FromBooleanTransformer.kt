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

class FromBooleanTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        src ?: return null
        return when (dbColumnDefinition.dataType) {
            TINYINT -> (if (src == true) 1 else 0).toByte()
            SMALLINT -> (if (src == true) 1 else 0).toShort()
            INTEGER -> if (src == true) 1 else 0
            BIGINT -> if (src == true) 1L else 0L
            else -> src
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(BOOLEAN)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT)
    }
}
