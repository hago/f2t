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

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.transform;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.Quintet;
import com.hagoapp.f2t.compare.ColumnComparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.time.*;
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
                    "2021-03-15T14:42:53.000001234Z"
            ),
            new Quintet<>(
                    LocalDateTime.of(2021, 3, 15, 14, 42, 53, 1234),
                    new Quintet<>(TIMESTAMP, 0, 0, 0, true),
                    Set.of(TIMESTAMP),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "2021-03-15T14:42:53.000001234"
            ),
            new Quintet<>(
                    LocalDate.of(2021, 3, 15),
                    new Quintet<>(DATE, 0, 0, 0, true),
                    Set.of(DATE),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "2021-03-15"
            ),
            new Quintet<>(
                    LocalTime.of(10, 23, 1, 567),
                    new Quintet<>(TIME, 0, 0, 0, true),
                    Set.of(TIME),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    "10:23:01.000000567"
            ),
            new Quintet<>(
                    null,
                    new Quintet<>(NCLOB, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(NVARCHAR, 2, 0, 0, true),
                    null
            )
    );

    private final Logger logger = LoggerFactory.getLogger(ToStringTransformTest.class);

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

            var targetCol = item.getFourth();
            var targetColDef = new ColumnDefinition();
            targetColDef.setDataType(targetCol.getFirst());
            var targetTm = targetColDef.getTypeModifier();
            targetTm.setMaxLength(targetCol.getSecond());
            targetTm.setPrecision(targetCol.getThird());
            targetTm.setScale(targetCol.getFourth());
            targetTm.setNullable(targetCol.getFifth());

            logger.debug("test {} -> {}", fileCol.getFirst(), targetCol.getFirst());
            var result = ColumnComparator.transform(src, fileColDef, targetColDef);
            Assertions.assertEquals(item.getFifth(), result);
        }
    }
}
