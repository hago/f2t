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

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.Set;

class Int2FloatTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.INTEGER),
                    new ColumnDefinition("", JDBCType.FLOAT),
                    1, 1.0f, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.INTEGER),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    -1, -1.0d, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BIGINT),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    2L, BigDecimal.valueOf(2.0d), null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.BIGINT),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    null, null, null, null
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
