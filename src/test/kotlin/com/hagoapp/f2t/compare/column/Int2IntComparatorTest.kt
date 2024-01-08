package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.sql.JDBCType

class Int2IntComparatorTest {

    private val cases = listOf(
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.TINYINT),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = true, false),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.SMALLINT),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = true, false),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = true, false),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.INTEGER),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = true, false),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = true, false),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = true, false),
            null, null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.BIGINT),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = true, true),
            null, null
        ),
    )

    @Test
    fun testComparison() {
        assertDoesNotThrow {
            ComparatorCase.runCases(cases)
        }
    }
}