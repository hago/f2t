/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.transform;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.Quintet;
import com.hagoapp.f2t.compare.ColumnComparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.sql.Time;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import static java.sql.JDBCType.*;

class ToStringTransformTest {

    /**
     * value
     * source column definition
     * source column possible types
     * target column definition
     * result expect
     */
    private final List<Quintet<
            Object,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Set<JDBCType>,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Object
            >> cases = List.of(
            new Quintet<>(
                    "abc",
                    new Quintet<>(NCLOB, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "abc"
            ),
            new Quintet<>(
                    1,
                    new Quintet<>(INTEGER, 0, 0, 0, true),
                    Set.of(TINYINT, SMALLINT, INTEGER, BIGINT),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "1"
            ),
            new Quintet<>(
                    2.3F,
                    new Quintet<>(FLOAT, 0, 0, 0, true),
                    Set.of(FLOAT, DOUBLE, DECIMAL),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "2.3"
            ),
            new Quintet<>(
                    647380920.3223D,
                    new Quintet<>(DOUBLE, 0, 0, 0, true),
                    Set.of(FLOAT, DOUBLE, DECIMAL),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    Double.valueOf(647380920.3223D).toString()
            ),
            new Quintet<>(
                    true,
                    new Quintet<>(BOOLEAN, 0, 0, 0, true),
                    Set.of(BOOLEAN),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "true"
            ),
            new Quintet<>(
                    ZonedDateTime.of(2021, 3, 15,
                            14, 42, 53, 1234, ZoneId.of("UTC")),
                    new Quintet<>(TIMESTAMP_WITH_TIMEZONE, 0, 0, 0, true),
                    Set.of(TIMESTAMP_WITH_TIMEZONE),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "2021-03-15T14:42:53.000001234Z[UTC]"
            ),
            new Quintet<>(
                    ZonedDateTime.of(2021, 3, 15,
                            14, 42, 53, 1234, ZoneId.of("UTC")).toLocalDate(),
                    new Quintet<>(DATE, 0, 0, 0, true),
                    Set.of(DATE),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "2021-03-15"
            ),
            new Quintet<>(
                    new Time(1234L + 23 * 60 * 1000L + 2 * 3600 * 1000L),
                    new Quintet<>(TIME, 0, 0, 0, true),
                    Set.of(TIME),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "10:23:01"
            ),
            new Quintet<>(
                    null,
                    new Quintet<>(NCLOB, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    null
            )
    );

    @Test
    void testTransform() {
        for (var item : cases) {
            var src = item.getFirst();
            var fileCol = item.getSecond();
            var fileColDef = new FileColumnDefinition();
            fileColDef.setDataType(fileCol.getFirst());
            var srcTm = fileColDef.getTypeModifier();
            srcTm.setMaxLength(fileCol.getSecond());
            srcTm.setPrecision(fileCol.getThird());
            srcTm.setScale(fileCol.getFourth());
            srcTm.setNullable(fileCol.getFifth());
            fileColDef.setPossibleTypes(item.getThird());

            var destCol = item.getFourth();
            var destColDef = new ColumnDefinition();
            destColDef.setDataType(destCol.getFirst());
            var destTm = destColDef.getTypeModifier();
            destTm.setMaxLength(destCol.getSecond());
            destTm.setPrecision(destCol.getThird());
            destTm.setScale(destCol.getFourth());
            destTm.setNullable(destCol.getFifth());

            var result = ColumnComparator.Companion.transform(src, fileColDef, destColDef);
            System.out.println(item.getFifth());
            System.out.println(result);
            Assertions.assertEquals(item.getFifth(), result);
        }
    }
}
