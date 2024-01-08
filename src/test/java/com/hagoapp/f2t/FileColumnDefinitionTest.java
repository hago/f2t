/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.HashSet;
import java.util.Set;

public class FileColumnDefinitionTest {

    private static FileColumnDefinition col1;
    private static FileColumnDefinition col2;

    @BeforeAll
    public static void init() {
        col1 = createDefaultDefinition();
        col2 = createDefaultDefinition();
    }

    private static FileColumnDefinition createDefaultDefinition() {
        var col = new FileColumnDefinition("col", Set.of(JDBCType.INTEGER, JDBCType.VARCHAR), JDBCType.VARCHAR);
        col.setMaximum(new BigDecimal(100));
        col.setMinimum(new BigDecimal(-100));
        col.setOrder(1);
        col.setContainsEmpty(true);
        col.setTypeModifier(new ColumnTypeModifier(
                10, 3, 0, null, false, false));
        return col;
    }

    @Test
    void testHashCode() {
        Assertions.assertDoesNotThrow(() -> col1.hashCode());
    }

    @Test
    void testEquals() {
        Assertions.assertEquals(col1, col2);
        col2.setTypeModifier(null);
        Assertions.assertNotEquals(col1, col2);

        col2 = createDefaultDefinition();
        col2.setMaximum(new BigDecimal(200));
        Assertions.assertNotEquals(col1, col2);

        col2 = createDefaultDefinition();
        col2.setMinimum(new BigDecimal(-200));
        Assertions.assertNotEquals(col1, col2);

        col2 = createDefaultDefinition();
        col2.setOrder(2);
        Assertions.assertNotEquals(col1, col2);

        col2 = createDefaultDefinition();
        col2.setContainsEmpty(!col2.isContainsEmpty());
        Assertions.assertNotEquals(col1, col2);

        col2 = createDefaultDefinition();
        var types = new HashSet<>(col2.getPossibleTypes());
        types.add(JDBCType.BIGINT);
        col2.setPossibleTypes(types);
        Assertions.assertNotEquals(col1, col2);
    }
}
