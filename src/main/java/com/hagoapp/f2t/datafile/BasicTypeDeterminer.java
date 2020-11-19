/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.sql.JDBCType;
import java.util.Set;

public class BasicTypeDeterminer implements DataTypeDeterminer {
    @Override
    public JDBCType determineTypes(Set<JDBCType> types) {
        if ((types == null) || types.isEmpty()) {
            return JDBCType.CLOB;
        } else if (types.size() == 1) {
            return types.iterator().next();
        } else if (types.contains(JDBCType.DOUBLE) || types.contains(JDBCType.FLOAT)
                || types.contains(JDBCType.DECIMAL)) {
            return JDBCType.DOUBLE;
        } else if (types.contains(JDBCType.INTEGER) || types.contains(JDBCType.BIGINT)) {
            return JDBCType.BIGINT;
        } else if (types.contains(JDBCType.BOOLEAN)) {
            return JDBCType.BOOLEAN;
        } else if (types.contains(JDBCType.TIMESTAMP)) {
            return JDBCType.TIMESTAMP;
        } else {
            return JDBCType.CLOB;
        }
    }
}
