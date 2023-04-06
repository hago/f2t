/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.DataCell;
import com.hagoapp.f2t.datafile.parquet.ParquetDataFileReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class ParquetFileReadTest {
    private static final List<String> testConfigFiles = List.of(
            "./tests/parquet/shuihudata_most.json",
            "./tests/parquet/shuihudata_least.json"
    );

    private static final List<ParquetTestConfig> testConfigs = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(ParquetFileReadTest.class);

    @BeforeAll
    public static void loadConfig() throws IOException {
        for (var testConfigFile : testConfigFiles) {
            try (FileInputStream fis = new FileInputStream(testConfigFile)) {
                String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                var testConfig = new Gson().fromJson(json, ParquetTestConfig.class);
                String realName = new File(System.getProperty("user.dir"),
                        Objects.requireNonNull(testConfig.getFileInfo().getFilename())).getAbsolutePath();
                logger.debug(realName);
                testConfig.getFileInfo().setFilename(realName);
                logger.debug(testConfig.toString());
                testConfigs.add(testConfig);
            }
        }
    }

    @Test
    public void readParquet() throws IOException {
        for (var testConfig : testConfigs) {
            try (var reader = new ParquetDataFileReader()) {
                reader.open(testConfig.getFileInfo());
                var columns = reader.findColumns().stream().map(ColumnDefinition::getName).collect(Collectors.toSet());
                var expectedColumns = testConfig.getExpect().getTypes().keySet();
                Assertions.assertEquals(testConfig.getExpect().getColumnCount(), columns.size());
                Assertions.assertTrue(expectedColumns.containsAll(columns));
                var count = 0;
                while (reader.hasNext()) {
                    var row = reader.next();
                    Assertions.assertEquals(expectedColumns.size(), row.getCells().size());
                    logger.debug("row {}: {}", count,
                            row.getCells().stream().map(DataCell::getData).collect(Collectors.toList()));
                    count++;
                }
                Assertions.assertEquals(testConfig.getExpect().getRowCount(), count);
            }
        }
    }
}
