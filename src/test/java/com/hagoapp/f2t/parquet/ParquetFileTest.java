/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
import com.hagoapp.f2t.*;
import com.hagoapp.f2t.csv.CsvTestConfig;
import com.hagoapp.f2t.datafile.DataTypeDeterminer;
import com.hagoapp.f2t.datafile.LeastTypeDeterminer;
import com.hagoapp.f2t.datafile.MostTypeDeterminer;
import com.hagoapp.f2t.datafile.parquet.FileInfoParquet;
import com.hagoapp.f2t.datafile.parquet.ParquetWriter;
import com.hagoapp.f2t.datafile.parquet.ParquetWriterConfig;
import kotlin.Triple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ParquetFileTest {

    private static final List<Triple<String, DataTypeDeterminer, String>> testConfigFiles = List.of(
            new Triple<>("./tests/csv/shuihudata.json", new MostTypeDeterminer(), "shuihu_most.parquet"),
            new Triple<>("./tests/csv/shuihudata_least.json", new LeastTypeDeterminer(), "shuihu_least.parquet")
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

    @Test
    @Order(value = 1)
    public void testWriteParquet() throws IOException, F2TException {
        for (var item : testConfigFiles) {
            var testConfigFile = item.getFirst();
            try (FileInputStream fis = new FileInputStream(testConfigFile)) {
                String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                var testConfig = new Gson().fromJson(json, CsvTestConfig.class);
                var parser = new FileParser(testConfig.getFileInfo());
                parser.setDefaultDeterminer(item.getSecond());
                var data = parser.extractData();
                var pwConfig = new ParquetWriterConfig("com.hagoapp.f2t", "shuihu", item.getThird());
                new ParquetWriter(data, pwConfig).write();
            }
        }
    }

    @Test
    @Order(value = 2)
    public void testReadParquet() throws IOException, F2TException {
        //var observer = new FileTestObserver();
        for (var item : testConfigFiles) {
            var testConfigFile = item.getFirst();
            try (FileInputStream fis = new FileInputStream(testConfigFile)) {
                String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                var testConfig = new Gson().fromJson(json, CsvTestConfig.class);
                var parquet = item.getThird();
                var parquetConfig = new FileInfoParquet();
                parquetConfig.setFilename(parquet);
                var parser = new FileParser(parquetConfig);
                //parser.addObserver(observer);
                parser.parse();
                var d = parser.extractData();
                Assertions.assertEquals(testConfig.getExpect().getColumnCount(), d.getColumnDefinition().size());
                //Assertions.assertEquals(testConfig.getExpect().getRowCount(), d.getRows().size());
            }
        }
    }
}
