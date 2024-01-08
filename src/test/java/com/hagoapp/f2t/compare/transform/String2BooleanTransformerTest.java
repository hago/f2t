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

class String2BooleanTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CLOB),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "true", true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "YES", true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "yEs", true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "y", true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "t", true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CLOB),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "false", false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "no", false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "No", false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "n", false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CHAR),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    "F", false, null, null
            ),
    };

    @Test
    void testTransform() {
        Assertions.assertNotEquals(0, cases.length);
        TransformCase.runCases(cases);
    }
}
