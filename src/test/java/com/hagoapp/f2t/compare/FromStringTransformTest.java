/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.Quintet;
import kotlin.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.*;
import java.time.chrono.ChronoZonedDateTime;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static java.sql.JDBCType.*;

class FromStringTransformTest {

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
            >> numberCases = List.of(
            new Quintet<>(
                    "11212",
                    new Quintet<>(NCLOB, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(INTEGER, 2, 0, 0, true),
                    11212
            ),
            new Quintet<>(
                    "6785.9999",
                    new Quintet<>(CLOB, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(FLOAT, 2, 0, 0, true),
                    6785.9999F
            ),
            new Quintet<>(
                    "784300016778.34567",
                    new Quintet<>(VARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(DOUBLE, 2, 0, 0, true),
                    784300016778.34567
            ),
            new Quintet<>(
                    "78430051115678922222016778.34567907547",
                    new Quintet<>(VARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(DECIMAL, 2, 0, 0, true),
                    new BigDecimal("78430051115678922222016778.34567907547")
            )
    );

    @Test
    void testNumber() {
        doTest(numberCases);
    }

    private void doTest(List<Quintet<
            Object,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Set<JDBCType>,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Object
            >> cases) {
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

    private final List<Quintet<
            Object,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Set<JDBCType>,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Object
            >> boolCases = List.of(
            new Quintet<>(
                    "false",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(BOOLEAN, 2, 0, 0, true),
                    false
            ),
            new Quintet<>(
                    "true",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(BOOLEAN, 2, 0, 0, true),
                    true
            ),

            new Quintet<>(
                    "no",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(BOOLEAN, 2, 0, 0, true),
                    false
            ),
            new Quintet<>(
                    "yes",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(BOOLEAN, 2, 0, 0, true),
                    true
            )
    );

    @Test
    void testBool() {
        doTest(boolCases);
    }

    private final List<Quintet<
            Object,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Set<JDBCType>,
            Quintet<JDBCType, Integer, Integer, Integer, Boolean>,
            Object
            >> dateTimeCases = List.of(
            new Quintet<>(
                    "20:00:01.123",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(TIME, 2, 0, 0, true),
                    LocalTime.of(20, 0, 1, 123000000)
            ),
            new Quintet<>(
                    "2020-01-01",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(DATE, 2, 0, 0, true),
                    LocalDate.of(2020, 1, 1)
            ),
            new Quintet<>(
                    "2020-01-01 12:23:45",
                    new Quintet<>(NVARCHAR, 0, 0, 0, true),
                    Set.of(NCLOB),
                    new Quintet<>(TIMESTAMP_WITH_TIMEZONE, 2, 0, 0, true),
                    ZonedDateTime.of(2020, 1, 1, 12, 23, 45, 0, ZoneId.systemDefault())
            )
    );

    @Test
    void testDateTime() {
        var func = new BiFunction<Object, Object, Boolean>() {

            @Override
            public Boolean apply(Object o, Object o2) {
                if ((o instanceof LocalTime) || (o instanceof LocalDate)) {
                    return o.equals(o2);
                } else if (o instanceof ZonedDateTime) {
                    return ((ZonedDateTime) o).isEqual((ZonedDateTime) o2);
                } else {
                    throw new UnsupportedOperationException("unknown type: " + o.getClass().getCanonicalName());
                }
            }
        };
        for (var item : dateTimeCases) {
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
            Assertions.assertTrue(func.apply(item.getFifth(), result));
        }
    }
}
