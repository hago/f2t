/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.TableDefinition;
import com.hagoapp.f2t.compare.TableDefinitionComparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.List;
import java.util.Set;

class TableDefinitionComparatorTest {

    private final ColumnDefinition INT_DB_COL = new ColumnDefinition("intCol", JDBCType.INTEGER);
    private final ColumnDefinition BIGINT_DB_COL = new ColumnDefinition("bigIntCol", JDBCType.BIGINT);
    private final ColumnDefinition FLOAT_DB_COL = new ColumnDefinition("floatCol", JDBCType.FLOAT);
    private final ColumnDefinition DOUBLE_DB_COL = new ColumnDefinition("doubleCol", JDBCType.DOUBLE);
    private final ColumnDefinition STRING_DB_COL = new ColumnDefinition("stringCol", JDBCType.VARCHAR);
    private final ColumnDefinition BOOLEAN_DB_COL = new ColumnDefinition("boolCol", JDBCType.BOOLEAN);
    private final ColumnDefinition TIMESTAMP_DB_COL = new ColumnDefinition("timestampCol", JDBCType.TIMESTAMP);

    private final FileColumnDefinition INT_FILE_COL = new FileColumnDefinition(
            "IntCol",
            Set.of(JDBCType.INTEGER)
    );
    private final FileColumnDefinition BIGINT_FILE_COL = new FileColumnDefinition(
            "BigIntCol",
            Set.of(JDBCType.BIGINT)
    );
    private final FileColumnDefinition FLOAT_FILE_COL = new FileColumnDefinition(
            "FloatCol",
            Set.of(JDBCType.FLOAT)
    );
    private final FileColumnDefinition DOUBLE_FILE_COL = new FileColumnDefinition(
            "DoubleCol",
            Set.of(JDBCType.DOUBLE)
    );
    private final FileColumnDefinition STRING_FILE_COL = new FileColumnDefinition(
            "StringCol",
            Set.of(JDBCType.VARCHAR)
    );
    private final FileColumnDefinition BOOLEAN_FILE_COL = new FileColumnDefinition(
            "BoolCol",
            Set.of(JDBCType.BOOLEAN)
    );
    private final FileColumnDefinition TIMESTAMP_FILE_COL = new FileColumnDefinition(
            "TimestampCol",
            Set.of(JDBCType.TIMESTAMP)
    );

    @Test
    void testCompare() {
        var dbColumns = List.of(
                INT_DB_COL, BIGINT_DB_COL, FLOAT_DB_COL, DOUBLE_DB_COL, STRING_DB_COL, BOOLEAN_DB_COL, TIMESTAMP_DB_COL
        );
        var fileColumns = List.of(
                INT_FILE_COL, BIGINT_FILE_COL, FLOAT_FILE_COL, DOUBLE_FILE_COL, STRING_FILE_COL, BOOLEAN_FILE_COL,
                TIMESTAMP_FILE_COL
        );
        var tblDef = new TableDefinition<>(dbColumns, true, null, false);
        var fileDef = new TableDefinition<>(fileColumns, false, null, false);
        var x = TableDefinitionComparator.compare(fileDef, tblDef);
        Assertions.assertFalse(x.isOfSameSchema());
        tblDef = new TableDefinition<>(dbColumns, false, null, false);
        x = TableDefinitionComparator.compare(fileDef, tblDef);
        Assertions.assertTrue(x.isOfSameSchema());
    }


}
