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

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import com.hagoapp.f2t.Quartet;
import com.hagoapp.f2t.Quintet;
import com.hagoapp.f2t.compare.ColumnComparator;
import kotlin.Pair;
import kotlin.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.List;

import static java.sql.JDBCType.*;

class String2StringCompareTest {

    private final List<Triple<JDBCType, JDBCType, Boolean>> clobCases = List.of(
            new Triple<>(CLOB, CLOB, true),
            new Triple<>(CLOB, NCLOB, true),
            new Triple<>(NCLOB, CLOB, true),
            new Triple<>(NCLOB, NCLOB, true)
    );

    @Test
    void text2TextColumnTest() {
        for (var i : clobCases) {
            var fc = new FileColumnDefinition();
            fc.setDataType(i.getFirst());
            var dc = new ColumnDefinition();
            dc.setDataType(i.getSecond());
            var r = ColumnComparator.Companion.compare(fc, dc);
            Assertions.assertEquals(r.isTypeMatched() && r.getCanLoadDataFrom(), i.getThird());
        }
    }

    private final List<Quartet<JDBCType, Integer, JDBCType, Boolean>> stringClobCases = List.of(
            new Quartet<>(CHAR, 1000000, CLOB, true),
            new Quartet<>(VARCHAR, 99999999, NCLOB, true),
            new Quartet<>(VARCHAR, 798798739, CLOB, true),
            new Quartet<>(CHAR, Integer.MAX_VALUE, NCLOB, true)
    );

    @Test
    void string2TextColumnTest() {
        for (var i : stringClobCases) {
            var fc = new FileColumnDefinition();
            fc.setDataType(i.getFirst());
            fc.getTypeModifier().setMaxLength(i.getSecond());
            var dc = new ColumnDefinition();
            dc.setDataType(i.getThird());
            var r = ColumnComparator.Companion.compare(fc, dc);
            Assertions.assertEquals(r.isTypeMatched() && r.getCanLoadDataFrom(), i.getFourth());
        }
    }

    private final List<Quartet<JDBCType, JDBCType, Integer, Boolean>> clobStringCases = List.of(
            new Quartet<>(CLOB, CHAR, 1000000, false),
            new Quartet<>(CLOB, VARCHAR, 99999999, false),
            new Quartet<>(NCLOB, CHAR, 798798739, false),
            new Quartet<>(NCLOB, VARCHAR, Integer.MAX_VALUE, false)
    );

    @Test
    void text2StringColumnTest() {
        for (var i : clobStringCases) {
            System.out.println(i);
            var fc = new FileColumnDefinition();
            fc.setDataType(i.getFirst());
            var dc = new ColumnDefinition();
            dc.setDataType(i.getSecond());
            dc.getTypeModifier().setMaxLength(i.getThird());
            var r = ColumnComparator.Companion.compare(fc, dc);
            Assertions.assertEquals(r.isTypeMatched() && r.getCanLoadDataFrom(), i.getFourth());
        }
    }

    private final List<Quintet<JDBCType, Integer, JDBCType, Integer, Pair<Boolean, Boolean>>> stringStringCases = List.of(
            new Quintet<>(CHAR, 20, CHAR, 1000000, new Pair<>(true, true)),
            new Quintet<>(CHAR, Integer.MAX_VALUE, VARCHAR, 99999999, new Pair<>(true, false)),
            new Quintet<>(VARCHAR, Integer.MAX_VALUE, CHAR, 798798739, new Pair<>(true, false)),
            new Quintet<>(VARCHAR, 335, VARCHAR, Integer.MAX_VALUE, new Pair<>(true, true))
    );

    @Test
    void string2StringColumnTest() {
        for (var i : stringStringCases) {
            var fc = new FileColumnDefinition();
            fc.setDataType(i.getFirst());
            fc.getTypeModifier().setMaxLength(i.getSecond());
            var dc = new ColumnDefinition();
            dc.setDataType(i.getThird());
            dc.getTypeModifier().setMaxLength(i.getFourth());
            var r = ColumnComparator.Companion.compare(fc, dc);
            Assertions.assertEquals(i.getFifth().getFirst(), r.isTypeMatched());
            Assertions.assertEquals(i.getFifth().getSecond(), r.getCanLoadDataFrom());
        }
    }
}
