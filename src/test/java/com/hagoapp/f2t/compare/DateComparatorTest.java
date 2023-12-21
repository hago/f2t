/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.ColumnTypeModifier;
import com.hagoapp.f2t.FileColumnDefinition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Set;

class DateComparatorTest {

    private static class TestCase {
        private final FileColumnDefinition fileColumn;
        private final ColumnDefinition dbColumn;
        private final CompareColumnResult result;

        public TestCase(
                @NotNull FileColumnDefinition fileColumn,
                @NotNull ColumnDefinition dbColumn,
                @NotNull CompareColumnResult result,
                ColumnTypeModifier fileTypeModifier,
                ColumnTypeModifier dbTypeModifier) {
            this.fileColumn = fileColumn;
            this.dbColumn = dbColumn;
            this.result = result;
            if (fileTypeModifier != null) {
                this.fileColumn.setTypeModifier(fileTypeModifier);
            }
            if (dbTypeModifier != null) {
                this.dbColumn.setTypeModifier(dbTypeModifier);
            }
        }
    }

    private static final TestCase[] cases = new TestCase[]{
            new TestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.DATE),
                    new CompareColumnResult(true, true),
                    null, null
            )
    };

    private final Logger logger = LoggerFactory.getLogger(DateComparatorTest.class);

    @Test
    void testComparison() {
        for (var c : cases) {
            logger.debug("test {} -> {}", c.fileColumn.getDataType(), c.dbColumn.getDataType());
            var r = ColumnComparator.compare(c.fileColumn, c.dbColumn);
            Assertions.assertEquals(c.result, r);
        }
    }
}
