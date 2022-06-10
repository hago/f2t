/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.hagoapp.f2t.datafile.parquet.ParquetStreamReader;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class ParquetStreamReaderTest {

    private final List<String> fileNames = List.of(
            "tests/parquet/shuihu_least.parquet",
            "tests/parquet/shuihu_least.parquet"
    );

    @Test
    public void testFileStream() throws IOException {
        for (var f: fileNames) {
            try (var fis = new FileInputStream(f)) {
                var pr = new ParquetStreamReader(fis);
            }
        }
    }
}
