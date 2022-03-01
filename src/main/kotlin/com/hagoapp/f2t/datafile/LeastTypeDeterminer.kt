/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.ColumnTypeModifier
import java.sql.JDBCType

/**
 * A type determiner implementation which always tries to use a compatible data set with the minimum value range.
 */
class LeastTypeDeterminer : DataTypeDeterminer {
    override fun determineTypes(
        types: Set<JDBCType>,
        modifier: ColumnTypeModifier
    ): JDBCType {
        return if (types.isEmpty()) {
            if (modifier.isContainsNonAscii) JDBCType.NVARCHAR else JDBCType.NCLOB
        } else if (types.size == 1) {
            types.first()
        } else if (types.contains(JDBCType.DOUBLE) || types.contains(JDBCType.FLOAT) ||
            types.contains(JDBCType.DECIMAL) || types.contains(JDBCType.INTEGER) ||
            types.contains(JDBCType.SMALLINT) || types.contains(JDBCType.TINYINT) ||
            types.contains(JDBCType.BIGINT)
        ) {
            determineNumberType(types)
        } else if (types.contains(JDBCType.BOOLEAN)) {
            JDBCType.BOOLEAN
        } else if (types.contains(JDBCType.TIMESTAMP_WITH_TIMEZONE)) {
            JDBCType.TIMESTAMP_WITH_TIMEZONE
        } else if (types.contains(JDBCType.CHAR) || types.contains(JDBCType.VARCHAR) || types.contains(JDBCType.CLOB) ||
            types.contains(JDBCType.NCHAR) || types.contains(JDBCType.NVARCHAR) || types.contains(JDBCType.NCLOB)
        ) {
            determineTextType(modifier)
        } else {
            JDBCType.VARBINARY
        }
    }

    private fun determineTextType(modifier: ColumnTypeModifier): JDBCType {
        return if (modifier.isContainsNonAscii) {
            JDBCType.NVARCHAR
        } else {
            JDBCType.VARCHAR
        }
    }

    private fun determineNumberType(types: Set<JDBCType>): JDBCType {
        return if (!types.contains(JDBCType.INTEGER) && !types.contains(JDBCType.SMALLINT) &&
            !types.contains(JDBCType.TINYINT) && !types.contains(JDBCType.BIGINT)
        ) {
            determineFloatPointType(types)
        } else {
            if (types.contains(JDBCType.TINYINT)) {
                JDBCType.TINYINT
            } else if (types.contains(JDBCType.SMALLINT)) {
                JDBCType.SMALLINT
            } else if (types.contains(JDBCType.INTEGER)) {
                JDBCType.INTEGER
            } else {
                JDBCType.BIGINT
            }
        }
    }

    private fun determineFloatPointType(types: Set<JDBCType>): JDBCType {
        return if (types.contains(JDBCType.FLOAT)) {
            JDBCType.FLOAT
        } else if (types.contains(JDBCType.DOUBLE)) {
            JDBCType.DOUBLE
        } else {
            JDBCType.DECIMAL
        }
    }
}
