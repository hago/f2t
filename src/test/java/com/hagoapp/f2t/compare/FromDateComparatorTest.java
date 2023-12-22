/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.ColumnTypeModifier;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.column.FromDateComparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

class FromDateComparatorTest {

    private static final DateTimeFormatter formatter = FromDateComparator.DEFAULT_DATE_FORMATTER;
    private static final ComparatorCase[] cases = new ComparatorCase[]{
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(formatter.format(ZonedDateTime.now()).length(), 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, false),
                    null,
                    new ColumnTypeModifier(formatter.format(ZonedDateTime.now()).length() - 1, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.VARCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(formatter.format(ZonedDateTime.now()).length(), 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.CLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.NCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(formatter.format(ZonedDateTime.now()).length(), 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.NVARCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(formatter.format(ZonedDateTime.now()).length(), 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.NCLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    new CompareColumnResult(false, false),
                    null, null
            )
    };

    @Test
    void testComparison() {
        Assertions.assertDoesNotThrow(() -> ComparatorCase.runCases(cases));
    }

    @Test
    void testFromDateComparatorWithCustomDateFormatter() {
        var customFormatterPattern = "YYYY";
        var customFormatter = DateTimeFormatter.ofPattern(customFormatterPattern);
        ComparatorCase[] formatCases = new ComparatorCase[]{
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                        new ColumnDefinition("", JDBCType.CHAR),
                        new CompareColumnResult(false, true),
                        null,
                        new ColumnTypeModifier(customFormatter.format(ZonedDateTime.now()).length(), 0, 0, null, false, false)
                ),
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                        new ColumnDefinition("", JDBCType.CHAR),
                        new CompareColumnResult(false, false),
                        null,
                        new ColumnTypeModifier(customFormatter.format(ZonedDateTime.now()).length() - 1, 0, 0, null, false, false)
                ),
        };
        var comparator = new FromDateComparator();
        for (var c : formatCases) {
            var r = comparator.dataCanLoadFrom(c.getFileColumn(), c.getDbColumn(), customFormatterPattern);
            Assertions.assertEquals(c.getResult().isTypeMatched(), r.isTypeMatched());
            Assertions.assertEquals(c.getResult().getCanLoadDataFrom(), r.getCanLoadDataFrom());
        }
    }
}
