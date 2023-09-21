/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.ColumnComparator;
import kotlin.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.List;

import static java.sql.JDBCType.*;

class NumericCompareTest {

    private final List<Triple<Triple<JDBCType, Integer, Integer>, JDBCType, Boolean>> float2IntCases = List.of(
            new Triple<>(new Triple<>(FLOAT, 2, 0), TINYINT, true),
            new Triple<>(new Triple<>(FLOAT, 4, 0), TINYINT, false),
            new Triple<>(new Triple<>(FLOAT, 4, 0), SMALLINT, true),
            new Triple<>(new Triple<>(FLOAT, 4, 1), SMALLINT, false),
            new Triple<>(new Triple<>(FLOAT, 6, 0), SMALLINT, false),
            new Triple<>(new Triple<>(FLOAT, String.valueOf(Integer.MAX_VALUE).length() - 1, 0), INTEGER, true),
            new Triple<>(new Triple<>(FLOAT, String.valueOf(Integer.MAX_VALUE).length() + 1, 0), INTEGER, false),
            new Triple<>(new Triple<>(FLOAT, String.valueOf(Long.MAX_VALUE).length() - 1, 0), BIGINT, true),
            new Triple<>(new Triple<>(FLOAT, String.valueOf(Long.MAX_VALUE).length() + 1, 0), BIGINT, false)
    );

    @Test
    void float2IntTest() {
        for (var i : float2IntCases) {
            var fileCol = new FileColumnDefinition();
            fileCol.setDataType(i.getFirst().getFirst());
            fileCol.getTypeModifier().setPrecision(i.getFirst().getSecond());
            fileCol.getTypeModifier().setScale(i.getFirst().getThird());
            var dbCol = new ColumnDefinition();
            dbCol.setDataType(i.getSecond());
            var result = ColumnComparator.Companion.compare(fileCol, dbCol);
            Assertions.assertEquals(i.getThird(), result.getCanLoadDataFrom());
        }
    }

    private final List<Triple<JDBCType, Triple<JDBCType, Integer, Integer>, Boolean>> int2FloatCases = List.of(
            new Triple<>(TINYINT, new Triple<>(DECIMAL, 2, 0), false),
            new Triple<>(TINYINT, new Triple<>(DECIMAL, 3, 0), true),
            new Triple<>(TINYINT, new Triple<>(DECIMAL, 3, 1), true),
            new Triple<>(SMALLINT, new Triple<>(DECIMAL, 4, 0), false),
            new Triple<>(SMALLINT, new Triple<>(DECIMAL, 5, 3), true),
            new Triple<>(INTEGER, new Triple<>(DECIMAL, String.valueOf(Integer.MAX_VALUE).length() - 1, 0), false),
            new Triple<>(INTEGER, new Triple<>(DECIMAL, String.valueOf(Integer.MAX_VALUE).length(), 0), true),
            new Triple<>(BIGINT, new Triple<>(DECIMAL, String.valueOf(Long.MAX_VALUE).length() - 1, 0), false),
            new Triple<>(BIGINT, new Triple<>(DECIMAL, String.valueOf(Long.MAX_VALUE).length(), 0), true),
            new Triple<>(BIGINT, new Triple<>(FLOAT, String.valueOf(Long.MAX_VALUE).length(), 0), true),
            new Triple<>(BIGINT, new Triple<>(DOUBLE, String.valueOf(Long.MAX_VALUE).length(), 0), true)
    );

    @Test
    void int2FloatTest() {
        for (var i : int2FloatCases) {
            var fileCol = new FileColumnDefinition();
            fileCol.setDataType(i.getFirst());
            var dbCol = new ColumnDefinition();
            dbCol.setDataType(i.getSecond().getFirst());
            dbCol.getTypeModifier().setPrecision(i.getSecond().getSecond());
            dbCol.getTypeModifier().setScale(i.getSecond().getThird());
            var result = ColumnComparator.Companion.compare(fileCol, dbCol);
            Assertions.assertEquals(i.getThird(), result.getCanLoadDataFrom());
        }
    }

    private final List<Triple<JDBCType, JDBCType, Boolean>> int2IntCases = List.of(
            new Triple<>(TINYINT, SMALLINT, true),
            new Triple<>(SMALLINT, SMALLINT, true),
            new Triple<>(SMALLINT, INTEGER, true),
            new Triple<>(INTEGER, BIGINT, true),
            new Triple<>(BIGINT, SMALLINT, false),
            new Triple<>(BIGINT, INTEGER, false),
            new Triple<>(INTEGER, SMALLINT, false),
            new Triple<>(SMALLINT, TINYINT, false)
    );

    @Test
    void int2IntTest() {
        for (var i : int2IntCases) {
            var fileCol = new FileColumnDefinition();
            fileCol.setDataType(i.getFirst());
            var dbCol = new ColumnDefinition();
            dbCol.setDataType(i.getSecond());
            var result = ColumnComparator.Companion.compare(fileCol, dbCol);
            Assertions.assertEquals(i.getThird(), result.getCanLoadDataFrom());
        }
    }

    private final List<Triple<Triple<JDBCType, Integer, Integer>, Triple<JDBCType, Integer, Integer>, Boolean>> float2FloatCases = List.of(
            new Triple<>(new Triple<>(FLOAT, 10, 2), new Triple<>(FLOAT, 4, 3), true),
            new Triple<>(new Triple<>(FLOAT, 10, 2), new Triple<>(FLOAT, 11, 3), true),
            new Triple<>(new Triple<>(FLOAT, 10, 5), new Triple<>(FLOAT, 4, 3), true),
            new Triple<>(new Triple<>(FLOAT, 10, 5), new Triple<>(FLOAT, 11, 4), true)
    );

    @Test
    void float2FloatTest() {
        for (var i : float2FloatCases) {
            var fileCol = new FileColumnDefinition();
            fileCol.setDataType(i.getFirst().getFirst());
            fileCol.getTypeModifier().setPrecision(i.getFirst().getSecond());
            fileCol.getTypeModifier().setScale(i.getFirst().getThird());
            var dbCol = new ColumnDefinition();
            dbCol.setDataType(i.getSecond().getFirst());
            dbCol.getTypeModifier().setPrecision(i.getSecond().getSecond());
            dbCol.getTypeModifier().setScale(i.getSecond().getThird());
            var result = ColumnComparator.Companion.compare(fileCol, dbCol);
            System.out.printf("test %s -> %s\n", i.getFirst().toString(), i.getSecond().toString());
            Assertions.assertEquals(i.getThird(), result.getCanLoadDataFrom());
        }
    }
}
