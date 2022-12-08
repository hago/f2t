/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.Constants;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.FileParser;
import com.hagoapp.f2t.csv.CsvTestConfig;
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer;
import com.hagoapp.f2t.datafile.FileTypeDeterminer;
import com.hagoapp.f2t.datafile.parquet.*;
import kotlin.Triple;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MemoryParquetReadTest {
    private static final List<Triple<String, FileColumnTypeDeterminer, String>> testConfigFiles = List.of(
            //new Triple<>("./tests/csv/shuihudata.json", FileColumnTypeDeterminer.Companion.getMostTypeDeterminer(), "shuihu_most.parquet"),
            new Triple<>("./tests/csv/shuihudata_least.json", FileColumnTypeDeterminer.Companion.getLeastTypeDeterminer(), "shuihu_least.parquet")
    );

    private static final Logger logger = LoggerFactory.getLogger(MemoryParquetReadTest.class);

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
    public void testParquetMemoryReader() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            var len = new File(config.getThird()).length();
            CsvTestConfig csvConfig;
            try (var fis = new FileInputStream(config.getFirst())) {
                var json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                csvConfig = new Gson().fromJson(json, CsvTestConfig.class);
            }
            try (var fis = new FileInputStream(config.getThird())) {
                try (var ps = MemoryParquetReader.create(fis, len)) {
                    var columns = ps.getColumns();
                    logger.debug("columns: {}", columns);
                    var rows = ps.read(csvConfig.getExpect().getRowCount() * 2);
                    Assertions.assertEquals(csvConfig.getExpect().getRowCount(), rows.length);
                }
            }
        }
    }

    @Test
    public void testParquetMemoryReaderSkip() throws IOException {
        Random random = new Random();
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            CsvTestConfig csvConfig;
            try (var fis = new FileInputStream(config.getFirst())) {
                var json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                csvConfig = new Gson().fromJson(json, CsvTestConfig.class);
            }
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetReader.create(bytes)) {
                    var rowCount = csvConfig.getExpect().getRowCount();
                    var skipCount = random.nextInt(rowCount);
                    ps.skip(skipCount);
                    var rows = ps.read(rowCount);
                    Assertions.assertEquals(rowCount - skipCount, rows.length);
                }
            }
        }
    }

    @Test
    public void testParquetMemoryReaderWithColumnNames() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            var len = new File(config.getThird()).length();
            CsvTestConfig csvConfig;
            try (var fis = new FileInputStream(config.getFirst())) {
                var json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                csvConfig = new Gson().fromJson(json, CsvTestConfig.class);
            }
            try (var fis = new FileInputStream(config.getThird())) {
                try (var ps = MemoryParquetReader.create(fis, len)) {
                    var columns = ps.getColumns();
                    logger.debug("columns: {}", columns);
                    var selectedColumnIndexes = selectRandomColumns(columns);
                    var columnNames = selectedColumnIndexes.stream().map(i -> columns.get(i).getName())
                            .toArray(String[]::new);
                    logger.debug("selected column indexes: {}", selectedColumnIndexes);
                    logger.debug("selected columns: {}", List.of(columnNames));
                    ps.fetchColumnByNames(columnNames);
                    var rows = ps.read(csvConfig.getExpect().getRowCount() * 2);
                    Assertions.assertEquals(csvConfig.getExpect().getRowCount(), rows.length);
                    for (Object[] row : rows) {
                        for (var i = 0; i < columns.size(); i++) {
                            if (!selectedColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    private List<Integer> selectRandomColumns(List<ColumnDefinition> columns) {
        Random random = new Random();
        var countNeeded = random.nextInt(columns.size() - 1) + 1;
        var indexes = IntStream.range(0, columns.size()).boxed().collect(Collectors.toList());
        var indexesNeeded = new ArrayList<Integer>();
        for (var i = 0; i < countNeeded; i++) {
            var j = random.nextInt(indexes.size());
            indexesNeeded.add(indexes.get(j));
            indexes.remove(j);
        }
        return indexesNeeded;
    }

    @Test
    public void testParquetMemoryReaderWithColumnNameSelector() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            var len = new File(config.getThird()).length();
            CsvTestConfig csvConfig;
            try (var fis = new FileInputStream(config.getFirst())) {
                var json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                csvConfig = new Gson().fromJson(json, CsvTestConfig.class);
            }
            try (var fis = new FileInputStream(config.getThird())) {
                try (var ps = MemoryParquetReader.create(fis, len)) {
                    var columns = ps.getColumns();
                    logger.debug("columns: {}", columns);
                    var selectedColumnIndexes = selectRandomColumns(columns);
                    var columnNames = selectedColumnIndexes.stream().map(i -> columns.get(i).getName())
                            .toArray(String[]::new);
                    logger.debug("selected column indexes: {}", selectedColumnIndexes);
                    logger.debug("selected columns: {}", List.of(columnNames));
                    ps.fetchColumnByNameSelector(s -> List.of(columnNames).contains(s));
                    var rows = ps.read(csvConfig.getExpect().getRowCount() * 2);
                    Assertions.assertEquals(csvConfig.getExpect().getRowCount(), rows.length);
                    for (Object[] row : rows) {
                        for (var i = 0; i < columns.size(); i++) {
                            if (!selectedColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testParquetMemoryReaderWithColumnIndexes() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            var len = new File(config.getThird()).length();
            CsvTestConfig csvConfig;
            try (var fis = new FileInputStream(config.getFirst())) {
                var json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                csvConfig = new Gson().fromJson(json, CsvTestConfig.class);
            }
            try (var fis = new FileInputStream(config.getThird())) {
                try (var ps = MemoryParquetReader.create(fis, len)) {
                    var columns = ps.getColumns();
                    logger.debug("columns: {}", columns);
                    var selectedColumnIndexes = selectRandomColumns(columns);
                    logger.debug("selected column indexes: {}", selectedColumnIndexes);
                    ps.fetchColumnByIndexes(selectedColumnIndexes.stream().mapToInt(i -> i).toArray());
                    var rows = ps.read(csvConfig.getExpect().getRowCount() * 2);
                    Assertions.assertEquals(csvConfig.getExpect().getRowCount(), rows.length);
                    for (Object[] row : rows) {
                        for (var i = 0; i < columns.size(); i++) {
                            if (!selectedColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testParquetMemoryReaderWithColumnIndexSelector() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            var len = new File(config.getThird()).length();
            CsvTestConfig csvConfig;
            try (var fis = new FileInputStream(config.getFirst())) {
                var json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                csvConfig = new Gson().fromJson(json, CsvTestConfig.class);
            }
            try (var fis = new FileInputStream(config.getThird())) {
                try (var ps = MemoryParquetReader.create(fis, len)) {
                    var columns = ps.getColumns();
                    logger.debug("columns: {}", columns);
                    var selectedColumnIndexes = selectRandomColumns(columns);
                    logger.debug("selected column indexes: {}", selectedColumnIndexes);
                    ps.fetchColumnByIndexSelector(selectedColumnIndexes::contains);
                    var rows = ps.read(csvConfig.getExpect().getRowCount() * 2);
                    Assertions.assertEquals(csvConfig.getExpect().getRowCount(), rows.length);
                    for (Object[] row : rows) {
                        for (var i = 0; i < columns.size(); i++) {
                            if (!selectedColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }
}
