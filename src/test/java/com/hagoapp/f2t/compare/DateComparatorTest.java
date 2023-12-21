/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.FileColumnDefinition;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.Set;

class DateComparatorTest {

    private static final ComparatorTestCase[] cases = new ComparatorTestCase[]{
            new ComparatorTestCase(
                    new FileColumnDefinition("", Set.of(), JDBCType.DATE),
                    new ColumnDefinition("", JDBCType.DATE),
                    new CompareColumnResult(true, true),
                    null, null
            )
    };

    @Test
    void testComparison() {
        ComparatorTestCase.runCases(cases);
    }
}
