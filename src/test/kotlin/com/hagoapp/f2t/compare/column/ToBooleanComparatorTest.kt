/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.sql.JDBCType

class ToBooleanComparatorTest {
    private val cases = listOf(
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BOOLEAN),
            ColumnDefinition("", JDBCType.BOOLEAN),
            CompareColumnResult(isTypeMatched = true, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(JDBCType.BOOLEAN), JDBCType.CHAR),
            ColumnDefinition("", JDBCType.BOOLEAN),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.CHAR),
            ColumnDefinition("", JDBCType.BOOLEAN),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
    )

    @Test
    fun testComparison() {
        Assertions.assertDoesNotThrow {
            ComparatorCase.runCases(cases)
        }
    }
}
