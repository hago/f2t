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

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.Set;

class Float2FloatTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.FLOAT),
                    1.1f, 1.1f, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    1.1f, (double) 1.1f, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    1.1f, (double) 1.1f, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    BigDecimal.valueOf(1.1d), BigDecimal.valueOf(1.1d).doubleValue(), null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DECIMAL),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    123f, BigDecimal.valueOf(123), null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.FLOAT),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    null, null, null, null
            )
    };

    @Test
    void testTransform() {
        Assertions.assertTrue(cases.length > 0);
        TransformCase.runCases(cases);
    }
}
