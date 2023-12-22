/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.ColumnTypeModifier;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.column.FromTimestampComparator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

class FromTimestampComparatorTest {

    private static final DateTimeFormatter formatter = FromTimestampComparator.DEFAULT_TIMESTAMP_FORMATTER;
    private static final int DEFAULT_MAX_LENGTH = formatter.format(
            LocalDateTime.of(2000, 1, 1, 1, 1, 1, 111111111)).length();
    private static final int DEFAULT_MAX_LENGTH_WITH_TIME_ZONE = DEFAULT_MAX_LENGTH + 5;
    private static final ComparatorCase[] cases = new ComparatorCase[]{
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, false),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH - 1, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.VARCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.CLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.NCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.NVARCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.NCLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.BOOLEAN),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH_WITH_TIME_ZONE, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.CHAR),
                    new CompareColumnResult(false, false),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH_WITH_TIME_ZONE - 1, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.VARCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH_WITH_TIME_ZONE, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.CLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.NCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH_WITH_TIME_ZONE, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.NVARCHAR),
                    new CompareColumnResult(false, true),
                    null,
                    new ColumnTypeModifier(DEFAULT_MAX_LENGTH_WITH_TIME_ZONE, 0, 0, null, false, false)
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.NCLOB),
                    new CompareColumnResult(false, true),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.INTEGER),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.DOUBLE),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.DECIMAL),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.SMALLINT),
                    new CompareColumnResult(false, false),
                    null, null
            ),
            new ComparatorCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                    new ColumnDefinition("", JDBCType.BIGINT),
                    new CompareColumnResult(false, false),
                    null, null
            ),
    };

    @Test
    void testComparison() {
        Assertions.assertDoesNotThrow(() -> ComparatorCase.runCases(cases));
    }

    private final Logger logger = LoggerFactory.getLogger(FromTimestampComparatorTest.class);
    private final LocalDateTime testLocalDt = LocalDateTime.of(2000, 1, 1, 1, 1, 1, 111111111);

    @Test
    void testFromTimestampComparatorWithCustomDateFormatter() {
        var customFormatterPattern = "yyyy-MM-dd HH:mm:ss.n";
        var customFormatter = DateTimeFormatter.ofPattern(customFormatterPattern);
        ComparatorCase[] formatCases = new ComparatorCase[]{
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                        new ColumnDefinition("", JDBCType.CHAR),
                        new CompareColumnResult(false, true),
                        null,
                        new ColumnTypeModifier(customFormatter.format(testLocalDt).length(), 0, 0, null, false, false)
                ),
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP),
                        new ColumnDefinition("", JDBCType.CHAR),
                        new CompareColumnResult(false, false),
                        null,
                        new ColumnTypeModifier(customFormatter.format(testLocalDt).length() - 1, 0, 0, null, false, false)
                ),
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                        new ColumnDefinition("", JDBCType.BOOLEAN),
                        new CompareColumnResult(false, false),
                        null, null
                ),
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                        new ColumnDefinition("", JDBCType.CHAR),
                        new CompareColumnResult(false, true),
                        null,
                        new ColumnTypeModifier(customFormatter.format(testLocalDt).length() + 5, 0, 0, null, false, false)
                ),
                new ComparatorCase(
                        new FileColumnDefinition("", Set.of(), JDBCType.TIMESTAMP_WITH_TIMEZONE),
                        new ColumnDefinition("", JDBCType.CHAR),
                        new CompareColumnResult(false, false),
                        null,
                        new ColumnTypeModifier(customFormatter.format(testLocalDt).length() + 4, 0, 0, null, false, false)
                ),
        };
        var comparator = new FromTimestampComparator();
        for (var c : formatCases) {
            logger.debug("test {} -> {}", c.getFileColumn().getDataType(), c.getDbColumn().getDataType());
            var r = comparator.dataCanLoadFrom(c.getFileColumn(), c.getDbColumn(), customFormatterPattern);
            Assertions.assertEquals(c.getResult().isTypeMatched(), r.isTypeMatched());
            Assertions.assertEquals(c.getResult().getCanLoadDataFrom(), r.getCanLoadDataFrom());
        }
    }
}
