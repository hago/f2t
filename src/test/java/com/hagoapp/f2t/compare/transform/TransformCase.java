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

package com.hagoapp.f2t.compare.transform;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.ColumnTypeModifier;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.ColumnComparator;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TransformCase {
    private final FileColumnDefinition fileColumn;
    private final ColumnDefinition dbColumn;
    private final Object src;
    private final Object expect;
    private final String[] extra;

    public FileColumnDefinition getFileColumn() {
        return fileColumn;
    }

    public ColumnDefinition getDbColumn() {
        return dbColumn;
    }

    public Object getExpect() {
        return expect;
    }

    public TransformCase(
            @NotNull FileColumnDefinition fileColumn,
            @NotNull ColumnDefinition dbColumn,
            Object src,
            Object expect,
            ColumnTypeModifier fileTypeModifier,
            ColumnTypeModifier dbTypeModifier,
            String... extra
    ) {
        this.fileColumn = fileColumn;
        this.dbColumn = dbColumn;
        this.src = src;
        this.expect = expect;
        this.extra = extra;
        if (fileTypeModifier != null) {
            this.fileColumn.setTypeModifier(fileTypeModifier);
        }
        if (dbTypeModifier != null) {
            this.dbColumn.setTypeModifier(dbTypeModifier);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TransformCase.class);

    public static void runCases(List<TransformCase> cases) {
        runCases(cases.toArray(TransformCase[]::new));
    }

    public static void runCases(TransformCase[] cases) {
        for (var c : cases) {
            logger.debug("test {} -> {}, input {} expect {}",
                    c.getFileColumn().getDataType(), c.getDbColumn().getDataType(), c.src, c.expect);
            var r = ColumnComparator.transform(c.src, c.getFileColumn(), c.getDbColumn(), c.extra);
            Assertions.assertEquals(c.expect, r);
        }
    }
}
