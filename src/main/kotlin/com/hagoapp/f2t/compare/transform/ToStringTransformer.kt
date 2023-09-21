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
import java.time.temporal.Temporal

/**
 * Transformer from any other types to string.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class ToStringTransformer : TypedColumnTransformer {
    override fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any? {
        return when (dbColumnDefinition.dataType) {
            DATE -> {
                src as Temporal? ?: return null
                val fmt = if (extra.isNotEmpty()) extra[0] else null
                DateTimeTypeUtils.getDateFormatter(fmt).format(src)
            }
            TIME -> {
                src as Temporal? ?: return null
                val fmt = if (extra.isNotEmpty()) extra[0] else null
                DateTimeTypeUtils.getDTimeFormatter(fmt).format(src)
            }
            TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> {
                src as Temporal? ?: return null
                val fmt = if (extra.isNotEmpty()) extra[0] else null
                DateTimeTypeUtils.getDateTimeFormatter(fmt).format(src)
            }
            else -> src?.toString()
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(
            BOOLEAN,
            SMALLINT, TINYINT, INTEGER, BIGINT,
            FLOAT, DOUBLE, DECIMAL,
            DATE, TIME, TIMESTAMP, TIMESTAMP_WITH_TIMEZONE
        )
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB)
    }
}
