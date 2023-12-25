package com.hagoapp.f2t.compare.column


import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.CompareColumnResult
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.JDBCType

class String2FloatComparatorTest {

    private val cases = listOf(
        ComparatorCase(
            FileColumnDefinition("", setOf(), JDBCType.CLOB),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Float.MIN_VALUE.toBigDecimal().minus(BigDecimal.ONE),
                null
            ),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, false),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Float.MIN_VALUE.toBigDecimal(),
                Float.MAX_VALUE.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.FLOAT),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Double.MIN_VALUE.toBigDecimal(),
                Double.MAX_VALUE.toBigDecimal()
            ),
            ColumnDefinition("", JDBCType.DOUBLE),
            CompareColumnResult(isTypeMatched = false, true),
            null,
            null
        ),
        ComparatorCase(
            FileColumnDefinition(
                "",
                setOf(),
                JDBCType.CLOB,
                Double.MIN_VALUE.toBigDecimal(),
                Double.MAX_VALUE.toBigDecimal().plus(BigDecimal.ONE)
            ),
            ColumnDefinition("", JDBCType.DOUBLE),
            CompareColumnResult(isTypeMatched = false, false),
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