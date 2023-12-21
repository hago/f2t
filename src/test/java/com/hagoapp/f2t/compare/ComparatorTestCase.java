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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class ComparatorTestCase {
    private final FileColumnDefinition fileColumn;
    private final ColumnDefinition dbColumn;
    private final CompareColumnResult result;

    public FileColumnDefinition getFileColumn() {
        return fileColumn;
    }

    public ColumnDefinition getDbColumn() {
        return dbColumn;
    }

    public CompareColumnResult getResult() {
        return result;
    }

    public ComparatorTestCase(
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

    private static final Logger logger = LoggerFactory.getLogger(ComparatorTestCase.class);

    public static void runCases(List<ComparatorTestCase> cases) {
        runCases(cases.toArray(ComparatorTestCase[]::new));
    }

    public static void runCases(ComparatorTestCase[] cases) {
        for (var c : cases) {
            logger.debug("test {} -> {}", c.getFileColumn().getDataType(), c.getDbColumn().getDataType());
            var r = ColumnComparator.compare(c.getFileColumn(), c.getDbColumn());
            Assertions.assertEquals(c.getResult(), r);
        }
    }
}
