/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
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

    @Disabled
    @Test
    public void testMemoryParquetDataReader() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetDataReader.create(bytes)) {
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

    @Disabled
    @Test
    public void testMemoryParquetDataReaderSkip() throws IOException {
        Random random = new Random();
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetDataReader.create(bytes)) {
                    var skipCount = random.nextInt(108);
                    ps.skip(skipCount);
                    var rows = ps.read();
                    Assertions.assertEquals(108 - skipCount, rows.length);
                }
            }
        }
    }

    @Disabled
    @Test
    public void testMemoryParquetDataReaderWithColumnNames() throws IOException {
        Random random = new Random();
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetDataReader.create(bytes)) {
                    var columnNames = ps.getColumns();
                    var colCount = ps.getColumns().size();
                    var selectColCount = random.nextInt(colCount);
                    var pool = IntStream.range(0, colCount).boxed().collect(Collectors.toList());
                    var selectColumnNames = new ArrayList<String>();
                    var selectColumnIndexes = new ArrayList<Integer>();
                    for (int i = 0; i < selectColCount; i++) {
                        var j = random.nextInt(pool.size());
                        selectColumnNames.add(columnNames.get(pool.get(j)));
                        selectColumnIndexes.add(pool.get(j));
                        pool.remove(j);
                    }
                    ps.withColumnSelectByNames(selectColumnNames.toArray(new String[0]));
                    var rows = ps.read();
                    Assertions.assertEquals(108, rows.length);
                    for (int i : IntStream.range(0, colCount).toArray()) {
                        Assertions.assertEquals(colCount, rows[i].length);
                    }
                    for (var row : rows) {
                        for (int i = 0; i < row.length; i++) {
                            if (!selectColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            } else {
                                Assertions.assertNotNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    @Disabled
    @Test
    public void testMemoryParquetDataReaderWithColumnIndexes() throws IOException {
        Random random = new Random();
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetDataReader.create(bytes)) {
                    var colCount = ps.getColumns().size();
                    var selectColCount = random.nextInt(colCount);
                    var pool = IntStream.range(0, colCount).boxed().collect(Collectors.toList());
                    var selectColumnIndexes = new ArrayList<Integer>();
                    for (int i = 0; i < selectColCount; i++) {
                        var j = random.nextInt(pool.size());
                        selectColumnIndexes.add(pool.get(j));
                        pool.remove(j);
                    }
                    ps.withColumnSelectByIndexes(selectColumnIndexes.stream().mapToInt(Integer::intValue).toArray());
                    var rows = ps.read();
                    Assertions.assertEquals(108, rows.length);
                    for (int i : IntStream.range(0, colCount).toArray()) {
                        Assertions.assertEquals(colCount, rows[i].length);
                    }
                    for (var row : rows) {
                        for (int i = 0; i < row.length; i++) {
                            if (!selectColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            } else {
                                Assertions.assertNotNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    @Disabled
    @Test
    public void testMemoryParquetDataReaderWithColumnNameSelector() throws IOException {
        Random random = new Random();
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetDataReader.create(bytes)) {
                    var columnNames = ps.getColumns();
                    var colCount = ps.getColumns().size();
                    var selectColCount = random.nextInt(colCount);
                    var pool = IntStream.range(0, colCount).boxed().collect(Collectors.toList());
                    var selectColumnNames = new ArrayList<String>();
                    var selectColumnIndexes = new ArrayList<Integer>();
                    for (int i = 0; i < selectColCount; i++) {
                        var j = random.nextInt(pool.size());
                        selectColumnNames.add(columnNames.get(pool.get(j)));
                        selectColumnIndexes.add(pool.get(j));
                        pool.remove(j);
                    }
                    ps.withColumnNameSelector(selectColumnNames::contains);
                    var rows = ps.read();
                    Assertions.assertEquals(108, rows.length);
                    for (int i : IntStream.range(0, colCount).toArray()) {
                        Assertions.assertEquals(colCount, rows[i].length);
                    }
                    for (var row : rows) {
                        for (int i = 0; i < row.length; i++) {
                            if (!selectColumnIndexes.contains(i)) {
                                Assertions.assertNull(row[i]);
                            } else {
                                Assertions.assertNotNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }

    @Disabled
    @Test
    public void testMemoryParquetDataReaderWithColumnIndexSelector() throws IOException {
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = MemoryParquetDataReader.create(bytes)) {
                    var colCount = ps.getColumns().size();
                    ps.withColumnIndexSelector(i -> i % 2 == 0);
                    var rows = ps.read();
                    Assertions.assertEquals(108, rows.length);
                    for (int i : IntStream.range(0, colCount).toArray()) {
                        Assertions.assertEquals(colCount, rows[i].length);
                    }
                    for (var row : rows) {
                        for (int i = 0; i < row.length; i++) {
                            if (i % 2 != 0) {
                                Assertions.assertNull(row[i]);
                            } else {
                                Assertions.assertNotNull(row[i]);
                            }
                        }
                    }
                }
            }
        }
    }
}
