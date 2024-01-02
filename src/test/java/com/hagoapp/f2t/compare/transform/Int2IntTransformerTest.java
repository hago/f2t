/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.ColumnComparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.Set;

class Int2IntTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TINYINT),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    (byte)1, (short)1, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.SMALLINT),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    (short)-1, -1, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.INTEGER),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    2, 2L, null, null
            ),
    };

    @Test
    void testTransform() {
        Assertions.assertNotEquals(0, cases.length);
        TransformCase.runCases(cases);
        Assertions.assertThrows(F2TException.class, () -> {
            var c = new TransformCase(new FileColumnDefinition("", Set.of(), JDBCType.INTEGER),
                    new ColumnDefinition("", JDBCType.FLOAT),
                    "1", 1, null, null);
            ColumnComparator.transform(c.getSrc(), c.getFileColumn(), c.getDbColumn());
        });
    }
}
