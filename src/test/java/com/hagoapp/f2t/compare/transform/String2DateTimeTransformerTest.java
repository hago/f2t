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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Set;

class String2DateTimeTransformerTest {

    private final TransformCase[] cases = new TransformCase[]{
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.CLOB),
                    new ColumnDefinition("", JDBCType.TIMESTAMP),
                    "2021-01-01 12:34:56",
                    ZonedDateTime.of(2021, 1, 1, 12, 34, 56, 0, ZoneId.systemDefault()),
                    null, null
            ),
            new TransformCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.VARCHAR),
                    new ColumnDefinition("", JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    "2021-01-01T12:34:56Z",
                    ZonedDateTime.of(2021, 1, 1, 12, 34, 56, 0, ZoneId.of("GMT")),
                    null, null
            ),
    };

    @Test
    void testTransform() {
        Assertions.assertNotEquals(0, cases.length);
        var transformer = new String2DateTimeTransformer();
        for (var c: cases) {
            var t = transformer.transform(c.getSrc(), c.getFileColumn(), c.getDbColumn(), c.getExtra());
            Assertions.assertInstanceOf(ZonedDateTime.class, t);
            var z = (ZonedDateTime)t;
            var expect = (ZonedDateTime)c.getExpect();
            Assertions.assertTrue(expect.isEqual(z));
        }
    }
}
