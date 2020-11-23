/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import java.sql.JDBCType;
import java.util.Map;

public class Expect {
    private int rowCount;
    private int columnCount;
    private Map<String, JDBCType> types;

    public Map<String, JDBCType> getTypes() {
        return types;
    }

    public void setTypes(Map<String, JDBCType> types) {
        this.types = types;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    @Override
    public String toString() {
        return "Expect{" +
                "rowCount=" + rowCount +
                ", columnCount=" + columnCount +
                ", types=" + types +
                '}';
    }
}
