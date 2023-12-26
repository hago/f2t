/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

class BooleanTypeUtilsTest {

    private static class TestCase {
        private final String input;
        private final boolean convertible;
        private final Boolean value;

        public TestCase(String input, boolean convertible, Boolean value) {
            this.input = input;
            this.convertible = convertible;
            this.value = value;
        }
    }

    private final TestCase[] cases = new TestCase[]{
            new TestCase(null, true, false),
            new TestCase("true", true, true),
            new TestCase("false   ", true, false),
            new TestCase("y", true, true),
            new TestCase("yes", true, true),
            new TestCase("t", true, true),
            new TestCase("n", true, false),
            new TestCase("no", true, false),
            new TestCase("f", true, false),
            new TestCase("ok", false, false),

    };

    private final Logger logger = LoggerFactory.getLogger(BooleanTypeUtilsTest.class);

    @Test
    void testIsPossibleBooleanValue() {
        for (var c : cases) {
            logger.debug("test {}, expect the possibility of conversion is {}", c.input, c.convertible);
            Assertions.assertEquals(c.convertible, BooleanTypeUtils.isPossibleBooleanValue(c.input));
        }
    }

    @Test
    void toBoolean() {
        for (var c : cases) {
            logger.debug("test {}, expect converted boolean values is {}", c.input, c.value);
            Assertions.assertEquals(c.value, BooleanTypeUtils.toBoolean(c.input));
        }
    }
}
