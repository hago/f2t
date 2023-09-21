/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.hagoapp.f2t.datafile.parquet.MemoryParquetReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.hagoapp.f2t.parquet.LargeParquetTest.LARGE_PARQUET_TEST_FILE;

@EnabledIfSystemProperty(named = LARGE_PARQUET_TEST_FILE, matches = ".*?")
class LargeParquetTest {
    private static final Logger logger = LoggerFactory.getLogger(LargeParquetTest.class);
    public static final String LARGE_PARQUET_TEST_FILE = "f2t.parquet.file.2g";

    @Test
    void testLargeMemoryParquetOver2GBReading() throws IOException {
        var largeFileName = System.clearProperty(LARGE_PARQUET_TEST_FILE);
        var f = new File(largeFileName);
        try (var fs = new FileInputStream(f)) {
            try (var reader = MemoryParquetReader.create(fs, f.length())) {
                var columns = reader.getColumns();
                logger.debug("columns: {}", columns);
                var lines = reader.read(1000);
                Assertions.assertEquals(1000, lines.length);
                for (var line : lines) {
                    Assertions.assertEquals(columns.size(), line.length);
                }
            }
        }
    }
}
