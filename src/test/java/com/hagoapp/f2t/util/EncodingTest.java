/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.util.EncodingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class EncodingTest {
    @Test
    void testAsciiDetect() {
        Map.of(
                "张三", false,
                "1234abcL", true,
                "Ññ", false,
                "小さい", false,
                "한글", false
        ).forEach((text, result) -> Assertions.assertEquals(
                EncodingUtils.Companion.isAsciiText(text), result));
    }

    @Test
    void testCreateRandomString() {
        var len = 17;
        var s = EncodingUtils.createRandomString(len);
        Assertions.assertEquals(len, s.length());
        s = EncodingUtils.createRandomString(3, "x");
        Assertions.assertEquals("xxx", s);
    }

    @Test
    void testGuessEncoding() throws IOException {
        var expect = StandardCharsets.UTF_8.displayName();
        var file = "tests/csv/shuihudata.csv";
        Assertions.assertEquals(expect, EncodingUtils.guessEncoding(file));
        Assertions.assertEquals(expect, EncodingUtils.guessEncoding(new File(file)));
        try (var fis = new FileInputStream(file)) {
            var enc = EncodingUtils.guessEncoding(fis);
            Assertions.assertEquals(expect, enc);
        }
    }
}
