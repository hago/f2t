package com.hagoapp.f2t.compare.column


import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.sql.JDBCType

class Int2FloatComparatorTest {

    private val cases = listOf(
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.DOUBLE),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.DOUBLE),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.DOUBLE),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.DOUBLE),
            CompareColumnResult(isTypeMatched = false, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.DECIMAL),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier(0, 2, 1, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.DECIMAL),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            ColumnTypeModifier(0, 3, 1, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.DECIMAL),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            ColumnTypeModifier(0, 5, 1, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.DECIMAL),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier(0, 3, 1, null, false, false)
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.DECIMAL),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            ColumnTypeModifier(0, 10, 1, null, false, false)
        ),
    )

    @Test
    fun testComparison() {
        assertDoesNotThrow {
            ComparatorCase.runCases(cases)
        }
    }
}