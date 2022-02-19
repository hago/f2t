/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import java.sql.JDBCType;

/**
 * A Column definition for database table.
 *
 * @author suncjs
 */
public class ColumnDefinition {
    private String name;
    private JDBCType dataType = null;
    private final ColumnTypeModifier typeModifier = new ColumnTypeModifier();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JDBCType getDataType() {
        return dataType;
    }

    public void setDataType(JDBCType dataType) {
        this.dataType = dataType;
    }

    public ColumnTypeModifier getTypeModifier() {
        return typeModifier;
    }

    public ColumnDefinition() {
        //
    }

    public ColumnDefinition(String name) {
        this.name = name;
    }

    public ColumnDefinition(String name, JDBCType type) {
        this.name = name;
        this.dataType = type;
    }

    @Override
    public String toString() {
        return "ColumnDefinition{" +
                ", name='" + name + '\'' +
                ", inferredType=" + dataType +
                ", typeModifier=" + typeModifier + '\'' +
                '}';
    }
}
