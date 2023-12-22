/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.CompareColumnResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.Set;

class FromBooleanComparatorTest {

    private static final ComparatorCase[] cases = new ComparatorCase[]{
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.TINYINT),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    new CompareColumnResult(true, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.DATE),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.TIME),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.TIMESTAMP),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.TIMESTAMP),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.CLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.NVARCHAR),
                    new CompareColumnResult(false, true),
                    null, null
            )
    };

    @Test
    void testComparison() {
        Assertions.assertDoesNotThrow(() -> ComparatorCase.runCases(cases));
    }
}
