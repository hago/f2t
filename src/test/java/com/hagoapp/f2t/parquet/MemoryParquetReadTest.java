/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
import com.hagoapp.f2t.Constants;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.F2TLogger;
import com.hagoapp.f2t.FileParser;
import com.hagoapp.f2t.csv.CsvTestConfig;
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer;
import com.hagoapp.f2t.datafile.FileTypeDeterminer;
import com.hagoapp.f2t.datafile.parquet.MemoryParquetDataReader;
import com.hagoapp.f2t.datafile.parquet.ParquetWriter;
import com.hagoapp.f2t.datafile.parquet.ParquetWriterConfig;
import kotlin.Triple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class MemoryParquetReadTest {
    private static final List<Triple<String, FileColumnTypeDeterminer, String>> testConfigFiles = List.of(
            new Triple<>("./tests/csv/shuihudata.json", FileColumnTypeDeterminer.Companion.getMostTypeDeterminer(), "shuihu_most.parquet"),
            new Triple<>("./tests/csv/shuihudata_least.json", FileColumnTypeDeterminer.Companion.getLeastTypeDeterminer(), "shuihu_least.parquet")
    );

    private static final Logger logger = F2TLogger.getLogger();

    @AfterAll
    public static void clean() {
        if (!System.getProperties().contains(Constants.KEEP_PARQUET_FILE_GENERATED)) {
            testConfigFiles.forEach(item -> {
                try {
                    var b = new File(item.getThird()).delete();
                    logger.debug("{} deleted: {}", item.getThird(), b);
                } catch (SecurityException ignored) {
                    //
                }
            });
        }
    }

    @BeforeAll
    @Test
    static void testWriteParquet() throws IOException, F2TException {
        for (var item : testConfigFiles) {
            var testConfigFile = item.getFirst();
            try (FileInputStream fis = new FileInputStream(testConfigFile)) {
                String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                var testConfig = new Gson().fromJson(json, CsvTestConfig.class);
                var parser = new FileParser(testConfig.getFileInfo());
                parser.setDeterminer(new FileTypeDeterminer(item.getSecond()));
                var data = parser.extractData();
                var pwConfig = new ParquetWriterConfig("com.hagoapp.f2t", "shuihu", item.getThird());
                new ParquetWriter(data, pwConfig).write();
            }
        }
    }

    @Test
    public void testMemoryParquetDataReader() throws IOException {
        Random random = new Random(Instant.now().getEpochSecond());
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = new MemoryParquetDataReader(bytes)) {
                    var colCount = ps.getColumns().size();
                    var rows = ps.read();
                    Assertions.assertEquals(108, rows.length);
                    for (int i : IntStream.range(0, colCount).toArray()) {
                        Assertions.assertEquals(colCount, rows[i].length);
                    }
                }
            }
        }
    }
}
