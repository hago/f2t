package com.hagoapp.f2t.compare.column


import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.sql.JDBCType

class Int2StringComparatorTest {

    private val cases = listOf(
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.CLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.CLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.CLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.CLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.NCLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.NCLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.NCLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.NCLOB),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            ColumnTypeModifier("-128".length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier("127".length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            ColumnTypeModifier("-32767".length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier("32768".length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            ColumnTypeModifier(Integer.MIN_VALUE.toString().length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier(Integer.MAX_VALUE.toString().length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            ColumnTypeModifier(Long.MIN_VALUE.toString().length, 0, 0, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.CHAR),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier(Long.MAX_VALUE.toString().length, 0, 0, null, false, false)
        ),
    )

    @Test
    fun testComparison() {
        assertDoesNotThrow {
            ComparatorCase.runCases(cases)
        }
    }
}