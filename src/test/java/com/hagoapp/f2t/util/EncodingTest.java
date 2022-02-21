/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.util.EncodingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class EncodingTest {
    @Test
    public void testAsciiDetect() {
        Map.of(
                "张三", false,
                "1234abcL", true,
                "Ññ", false,
                "小さい", false,
                "한글", false
        ).forEach((text, result) -> Assertions.assertEquals(
                EncodingUtils.Companion.isAsciiText(text), result));
    }
}
