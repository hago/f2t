/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import java.sql.JDBCType

class BasicTypeDeterminer : DataTypeDeterminer {
    override fun determineTypes(types: Set<JDBCType>): JDBCType {
        return if (types.isEmpty()) {
            JDBCType.CLOB
        } else if (types.size == 1) {
            types.iterator().next()
        } else if (types.contains(JDBCType.DOUBLE) || types.contains(JDBCType.FLOAT)
            || types.contains(JDBCType.DECIMAL)
        ) {
            JDBCType.DOUBLE
        } else if (types.contains(JDBCType.INTEGER) || types.contains(JDBCType.BIGINT)) {
            JDBCType.BIGINT
        } else if (types.contains(JDBCType.BOOLEAN)) {
            JDBCType.BOOLEAN
        } else if (types.contains(JDBCType.TIMESTAMP)) {
            JDBCType.TIMESTAMP
        } else {
            JDBCType.CLOB
        }
    }
}
