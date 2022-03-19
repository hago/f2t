/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.csv;

import com.google.gson.Gson;
import com.hagoapp.f2t.*;
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer;
import com.hagoapp.f2t.datafile.FileTypeDeterminer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CsvReadTest {

    private static final Map<String, FileColumnTypeDeterminer> testConfigFiles = Map.of(
            "./tests/csv/shuihudata.json",
            FileColumnTypeDeterminer.Companion.getMostTypeDeterminer(),
            "./tests/csv/shuihudata_least.json",
            FileColumnTypeDeterminer.Companion.getLeastTypeDeterminer()
    );

    private static final Map<CsvTestConfig, FileTypeDeterminer> testConfigs = new HashMap<>();
    private static final Logger logger = F2TLogger.getLogger();

    @BeforeAll
    public static void loadConfig() throws IOException {
        for (var item : testConfigFiles.entrySet()) {
            var testConfigFile = item.getKey();
            try (FileInputStream fis = new FileInputStream(testConfigFile)) {
                String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                var testConfig = new Gson().fromJson(json, CsvTestConfig.class);
                String realCsv = new File(System.getProperty("user.dir"),
                        Objects.requireNonNull(testConfig.getFileInfo().getFilename())).getAbsolutePath();
                logger.debug(realCsv);
                testConfig.getFileInfo().setFilename(realCsv);
                logger.debug(testConfig.toString());
                testConfigs.put(testConfig, new FileTypeDeterminer(item.getValue()));
            }
        }
    }

    @Test
    public void readCsv() throws IOException {
        for (var item : testConfigs.entrySet()) {
            var testConfig = item.getKey();
            var determiner = item.getValue();
            logger.debug("start csv read test using {}", determiner.getClass().getCanonicalName());
            observer.setRowDetail(true);
            FileParser parser = new FileParser(testConfig.getFileInfo());
            parser.setDeterminer(determiner);
            parser.addObserver(observer);
            parser.parse();
            Assertions.assertEquals(testConfig.getExpect().getRowCount(), observer.getRowCount());
            Assertions.assertEquals(testConfig.getExpect().getColumnCount(), observer.getColumns().size());
            Assertions.assertEquals(testConfig.getExpect().getTypes(), observer.getColumns().values().stream()
                    .collect(Collectors.toMap(
                            p -> p.getFirst().getName(),
                            p -> p.getFirst().getDataType()
                    ))
            );
        }
    }

    FileTestObserver observer = new FileTestObserver();

    @Test
    public void extractCsv() throws IOException, F2TException {
        for (var item : testConfigs.entrySet()) {
            var testConfig = item.getKey();
            var determiner = item.getValue();
            FileParser parser = new FileParser(testConfig.getFileInfo());
            parser.setDeterminer(determiner);
            parser.addObserver(observer);
            var table = parser.extractData();
            Assertions.assertEquals(table.getRows().size(), testConfig.getExpect().getRowCount());
            Assertions.assertEquals(table.getColumnDefinition().size(), testConfig.getExpect().getColumnCount());
            Assertions.assertEquals(testConfig.getExpect().getTypes(), table.getColumnDefinition().stream()
                    .collect(Collectors.toMap(FileColumnDefinition::getName, FileColumnDefinition::getDataType))
            );
            System.out.println(table);
        }
    }
}
