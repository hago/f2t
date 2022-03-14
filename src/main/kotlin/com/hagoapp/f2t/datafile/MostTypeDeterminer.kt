/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.ColumnTypeModifier
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * A type determiner implementation which always tries to use a compatible data set with the maximum value range.
 */
class MostTypeDeterminer : DataTypeDeterminer {
    override fun determineTypes(
        types: Set<JDBCType>,
        modifier: ColumnTypeModifier
    ): JDBCType {
        return if (types.isEmpty()) {
            determineTextType(modifier)
        } else if (types.size == 1) {
            types.first()
        } else if (types.contains(DOUBLE) || types.contains(FLOAT) ||
            types.contains(DECIMAL) || types.contains(INTEGER) ||
            types.contains(SMALLINT) || types.contains(TINYINT) ||
            types.contains(BIGINT)
        ) {
            determineNumberType(types)
        } else if (types.contains(BOOLEAN)) {
            BOOLEAN
        } else if (types.contains(TIMESTAMP_WITH_TIMEZONE)) {
            TIMESTAMP_WITH_TIMEZONE
        } else if (types.contains(DATE)) {
            DATE
        } else if (types.contains(TIME) || types.contains(TIME_WITH_TIMEZONE)) {
            TIME_WITH_TIMEZONE
        } else if (types.contains(CHAR) || types.contains(VARCHAR) || types.contains(CLOB) ||
            types.contains(NCHAR) || types.contains(NVARCHAR) || types.contains(NCLOB)
        ) {
            determineTextType(modifier)
        } else {
            throw NotImplementedError("types $types not supported yet")
        }
    }

    private fun determineTextType(modifier: ColumnTypeModifier): JDBCType {
        return if (modifier.isContainsNonAscii) {
            NCLOB
        } else {
            CLOB
        }
    }

    private fun determineNumberType(types: Set<JDBCType>): JDBCType {
        return if (!types.contains(INTEGER) && !types.contains(SMALLINT) &&
            !types.contains(TINYINT) && !types.contains(BIGINT)
        ) {
            determineFloatPointType(types)
        } else {
            if (types.contains(BIGINT)) {
                BIGINT
            } else if (types.contains(INTEGER)) {
                INTEGER
            } else if (types.contains(SMALLINT)) {
                SMALLINT
            } else {
                TINYINT
            }
        }
    }

    private fun determineFloatPointType(types: Set<JDBCType>): JDBCType {
        return if (types.contains(DECIMAL)) {
            DECIMAL
        } else if (types.contains(DOUBLE)) {
            DOUBLE
        } else {
            FLOAT
        }
    }
}
