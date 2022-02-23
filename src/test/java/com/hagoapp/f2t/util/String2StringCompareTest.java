/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.compare.ColumnComparator;
import kotlin.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.List;

import static java.sql.JDBCType.*;

public class String2StringCompareTest {

    private final List<Triple<JDBCType, JDBCType, Boolean>> clobCases = List.of(
            new Triple<>(CLOB, CLOB, true),
            new Triple<>(CLOB, NCLOB, true),
            new Triple<>(NCLOB, CLOB, true),
            new Triple<>(NCLOB, NCLOB, true)
    );

    @Test
    public void text2TextColumnTest() {
        for (var i : clobCases) {
            var fc = new FileColumnDefinition();
            fc.setDataType(i.getFirst());
            var dc = new ColumnDefinition();
            dc.setDataType(i.getSecond());
            var r = ColumnComparator.Companion.compare(fc, dc);
            Assertions.assertTrue(r.isTypeMatched() && r.getCanLoadDataFrom());
        }
    }
}
