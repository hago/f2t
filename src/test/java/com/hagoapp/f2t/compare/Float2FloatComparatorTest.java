/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.ColumnTypeModifier;
import com.hagoapp.f2t.FileColumnDefinition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Set;

class Float2FloatComparatorTest {

    private static final ComparatorTestCase[] cases = new ComparatorTestCase[]{
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.FLOAT),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    new CompareColumnResult(true, false),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DOUBLE),
                    new ColumnDefinition("", JDBCType.FLOAT),
                    new CompareColumnResult(true, false),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DOUBLE),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DOUBLE),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    new CompareColumnResult(true, false),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DECIMAL),
                    new ColumnDefinition("", JDBCType.FLOAT),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DECIMAL),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DECIMAL),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    new CompareColumnResult(true, true),
                    null, null
            )
    };

    @Test
    void testComparison() {
        ComparatorTestCase.runCases(cases);
    }
}
