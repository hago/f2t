/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.util.NumericUtils;
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
}
