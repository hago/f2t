/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import java.sql.JDBCType
import java.sql.JDBCType.*

class ParquetTypeUtils {
    companion object {

        fun mapToAvroType(input: JDBCType): String {
            return when (input) {
                TINYINT, SMALLINT, INTEGER -> "int"
                BIGINT -> "long"
                BOOLEAN -> "boolean"
                FLOAT -> "float"
                DOUBLE, DECIMAL -> "double"
                BINARY, VARBINARY -> "bytes"
                TIME, TIME_WITH_TIMEZONE, DATE, TIMESTAMP_WITH_TIMEZONE, TIMESTAMP,
                CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB -> "string"
                else -> throw UnsupportedOperationException("unsupported type $input")
            }
        }
    }
}
