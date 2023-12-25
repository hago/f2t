/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import com.hagoapp.f2t.compare.TypedColumnComparator
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.SMALL
import org.slf4j.LoggerFactory
import java.sql.JDBCType
import java.sql.JDBCType.*
import kotlin.math.pow

/**
 * Compare int with string.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class Int2StringComparator : TypedColumnComparator {

    private val logger = LoggerFactory.getLogger(Int2StringComparator::class.java)

    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult {
        if ((dbColumnDefinition.dataType == CLOB) || (dbColumnDefinition.dataType == NCLOB)) {
            return CompareColumnResult(isTypeMatched = false, canLoadDataFrom = true)
        }
        return CompareColumnResult(
            isTypeMatched = false,
            canLoadDataFrom = maxLength(fileColumnDefinition.dataType) <= dbColumnDefinition.typeModifier.maxLength
        )
    }

    private fun maxLength(type: JDBCType): Int {
        return when (type) {
            TINYINT -> Byte.MIN_VALUE.toString().length
            SMALLINT -> 2.0.pow(16).toInt().toString().length + 1
            INTEGER -> Int.MIN_VALUE.toString().length
            BIGINT -> Long.MIN_VALUE.toString().length
            else -> {
                logger.error("{} shoud not be here, treat as long", type)
                Long.MIN_VALUE.toString().length
            }
        }
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(TINYINT, SMALLINT, INTEGER, BIGINT)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(CHAR, VARCHAR, NCHAR, NVARCHAR, CLOB, NCLOB)
    }

}