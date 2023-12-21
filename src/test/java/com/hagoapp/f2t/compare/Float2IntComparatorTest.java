/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.ColumnTypeModifier;
import com.hagoapp.f2t.FileColumnDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.Set;

class Float2IntComparatorTest {

    private static final ComparatorCase[] cases = new ComparatorCase[]{
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.TINYINT),
                    new CompareColumnResult(false, true),
                    new ColumnTypeModifier(0, 2, 0, null, false, false), null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.TINYINT),
                    new CompareColumnResult(false, false),
                    new ColumnTypeModifier(0, 2, 1, null, false, false), null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.TINYINT),
                    new CompareColumnResult(false, false),
                    new ColumnTypeModifier(0, 4, 0, null, false, false), null
            ),
    };

    @Test
    void testComparison() {
        Assertions.assertDoesNotThrow(() -> ComparatorCase.runCases(cases));
    }
}
