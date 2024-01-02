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

class String2StringTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CLOB),
                    new ColumnDefinition("", JDBCType.VARCHAR),
                    null, null, null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.VARCHAR),
                    "", "", null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.VARCHAR),
                    "3.1415926535897", "3.1415926535897", null, null
            ),
    };

    @Test
    void testTransform() {
        Assertions.assertNotEquals(0, cases.length);
        TransformCase.runCases(cases);
    }
}
