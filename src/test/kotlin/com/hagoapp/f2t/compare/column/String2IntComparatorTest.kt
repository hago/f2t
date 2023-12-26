package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.JDBCType

class String2IntComparatorTest {

    private val cases = listOf(
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.CLOB),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                (-129).toBigDecimal().minus(BigDecimal.ONE),
                null
            ),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                null,
                127.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                (-128).toBigDecimal(),
                127.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.TINYINT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.CLOB),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                (-32769).toBigDecimal().minus(BigDecimal.ONE),
                null
            ),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                null,
                32767.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                (-32768).toBigDecimal(),
                32767.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.SMALLINT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.VARCHAR),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Int.MIN_VALUE.toBigDecimal().minus(BigDecimal.ONE),
                null
            ),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                null,
                Int.MAX_VALUE.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Int.MIN_VALUE.toBigDecimal(),
                Int.MAX_VALUE.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.INTEGER),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.CLOB),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Long.MIN_VALUE.toBigDecimal().minus(BigDecimal.ONE),
                null
            ),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                null,
                Long.MAX_VALUE.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Long.MIN_VALUE.toBigDecimal(),
                Long.MAX_VALUE.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.BIGINT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
    )

    @Test
    fun testComparison() {
        assertDoesNotThrow {
            ComparatorCase.runCases(cases)
        }
    }
}