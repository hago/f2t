/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.Set;

class FromBooleanTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    true, true, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    false, false, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    true, 1L, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    false, 0L, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    true, 1, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    false, 0, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    true, (short) 1, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    false, (short) 0, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.TINYINT),
                    true, (byte) 1, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BOOLEAN),
                    new ColumnDefinition("", JDBCType.TINYINT),
                    false, (byte) 0, null, null
            )
    };

    @Test
    void testTransform() {
        Assertions.assertNotEquals(0, cases.length);
        TransformCase.runCases(cases);
    }
}
