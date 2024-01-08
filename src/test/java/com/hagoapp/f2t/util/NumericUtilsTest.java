/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.util.NumericUtils;
import kotlin.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class NumericUtilsTest {
    @Test
    void testIsDecimalLongValue() {
        var n = new BigDecimal(Long.MAX_VALUE);
        Assertions.assertTrue(NumericUtils.isDecimalLongValue(n));
        n = new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE);
        Assertions.assertFalse(NumericUtils.isDecimalLongValue(n));
        n = new BigDecimal(Float.MAX_VALUE);
        Assertions.assertFalse(NumericUtils.isDecimalLongValue(n));
        n = new BigDecimal(Double.MIN_VALUE);
        Assertions.assertFalse(NumericUtils.isDecimalLongValue(n));
        n = new BigDecimal("1.1");
        Assertions.assertFalse(NumericUtils.isDecimalLongValue(n));
    }

    @Test
    void testIsDecimalIntegerValue() {
        var n = new BigDecimal(Integer.MAX_VALUE);
        Assertions.assertTrue(NumericUtils.isDecimalIntegralValue(n));
        n = new BigDecimal(Long.MAX_VALUE);
        Assertions.assertFalse(NumericUtils.isDecimalIntegralValue(n));
        n = new BigDecimal(Float.MAX_VALUE);
        Assertions.assertFalse(NumericUtils.isDecimalIntegralValue(n));
        n = new BigDecimal(Double.MIN_VALUE);
        Assertions.assertFalse(NumericUtils.isDecimalIntegralValue(n));
        n = new BigDecimal("1.1");
        Assertions.assertFalse(NumericUtils.isDecimalIntegralValue(n));
    }

    @Test
    void testIsDecimalInFloatRange() {
        var n = new BigDecimal(String.valueOf(Float.MAX_VALUE));
        Assertions.assertTrue(NumericUtils.isDecimalInFloatRange(n));
        var n1 = n.add(BigDecimal.ONE);
        Assertions.assertFalse(NumericUtils.isDecimalInFloatRange(n1));
        n = BigDecimal.valueOf(Float.MIN_VALUE);
        Assertions.assertTrue(NumericUtils.isDecimalInFloatRange(n));
        n = n.subtract(BigDecimal.ONE);
        Assertions.assertFalse(NumericUtils.isDecimalInFloatRange(n));
    }

    @Test
    void testIsDecimalInDoubleRange() {
        var n = new BigDecimal(String.valueOf(Double.MAX_VALUE));
        Assertions.assertTrue(NumericUtils.isDecimalInDoubleRange(n));
        n = n.add(BigDecimal.ONE);
        Assertions.assertFalse(NumericUtils.isDecimalInDoubleRange(n));
        n = BigDecimal.valueOf(Double.MIN_VALUE);
        Assertions.assertTrue(NumericUtils.isDecimalInDoubleRange(n));
        n = n.subtract(BigDecimal.ONE);
        Assertions.assertFalse(NumericUtils.isDecimalInDoubleRange(n));
    }

    @Test
    void testDetectPrecision() {
        Assertions.assertEquals(new Pair<>(1, 1), NumericUtils.detectPrecision("3.2"));
        Assertions.assertEquals(new Pair<>(0, 0), NumericUtils.detectPrecision("abc"));
        Assertions.assertEquals(new Pair<>(0, 0), NumericUtils.detectPrecision(false));
        Assertions.assertEquals(new Pair<>(4, 0), NumericUtils.detectPrecision(1000));
        Assertions.assertEquals(new Pair<>(4, 1), NumericUtils.detectPrecision(1000.000));
        Assertions.assertEquals(new Pair<>(4, 1), NumericUtils.detectPrecision("1000.000"));
        Assertions.assertEquals(new Pair<>(7, 8), NumericUtils.detectPrecision(1234567.89076543));
        Assertions.assertEquals(new Pair<>(6, 3), NumericUtils.detectPrecision(-100008.567));
        Assertions.assertEquals(new Pair<>(0, 0), NumericUtils.detectPrecision(new Object()));
    }
}
