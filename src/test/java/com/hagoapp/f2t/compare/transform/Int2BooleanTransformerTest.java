/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.Set;

class Int2BooleanTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.INTEGER),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    1, true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.INTEGER),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    -1, false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BIGINT),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    2L, true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BIGINT),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    0L, false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.SMALLINT),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    (short) 1, true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.SMALLINT),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    (short) -1, false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TINYINT),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    (byte) 100, true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TINYINT),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    (byte) -128, false, null, null
            ),
    };

    @Test
    void testTransform() {
        Assertions.assertNotEquals(0, cases.length);
        TransformCase.runCases(cases);
    }
}
