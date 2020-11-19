/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnsDefinition {
    private List<ColumnDefinition> columns = new ArrayList<>();

    private ColumnDefinition findColumnDefinition(int index) {
        return columns.stream().filter(cd -> cd.getIndex() == index).findFirst().orElseGet(() -> null);
    }

    private ColumnDefinition findColumnDefinition(String name) {
        if (name == null) {
            throw new IllegalArgumentException("column name should not be null");
        }
        return columns.stream().filter(cd -> cd.getName().equals(name)).findFirst().orElseGet(() -> null);
    }

    public void addColumn(int columnIndex, String name) {
        ColumnDefinition cd = new ColumnDefinition(columnIndex, name);
        ColumnDefinition existed = findColumnDefinition(name);
        if ((existed != null) && (existed.getIndex() != columnIndex)) {
            throw new IllegalArgumentException(
                    String.format("column %d named'%s' existed with different index %d",
                            columnIndex, name, existed.getIndex()));
        }
        columns.add(new ColumnDefinition(columnIndex, name));
    }

    public void addType(int columnIndex, JDBCType type) {
        ColumnDefinition cd = findColumnDefinition(columnIndex);
        if (cd == null) {
            throw new IllegalArgumentException(String.format("column %d not found", columnIndex));
        }
        cd.getPossibleTypes().add(type);
    }

    public void addType(String name, JDBCType type) {
        ColumnDefinition cd = findColumnDefinition(name);
        if (cd == null) {
            throw new IllegalArgumentException(String.format("column '%s' not found", name));
        }
        cd.getPossibleTypes().add(type);
    }

    public List<ColumnDefinition> guessTypes() {
        return guessTypes(new BasicTypeDeterminer());
    }

    public List<ColumnDefinition> guessTypes(DataTypeDeterminer determiner) {
        return columns.stream().map(column -> {
            ColumnDefinition cd = new ColumnDefinition(column.getIndex(), column.getName());
            cd.setPossibleTypes(Set.of(determiner.determineTypes(column.getPossibleTypes())));
            return cd;
        }).collect(Collectors.toList());
    }
}
