/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.sql.JDBCType;
import java.util.HashSet;
import java.util.Set;

public class ColumnDefinition {
    private int index;
    private String name;
    private Set<JDBCType> possibleTypes = new HashSet<>();

    public ColumnDefinition(int i, String n) {
        index = i;
        name = n;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<JDBCType> getPossibleTypes() {
        return possibleTypes;
    }

    public void setPossibleTypes(Set<JDBCType> possibleTypes) {
        this.possibleTypes = possibleTypes;
    }
}
