/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
import com.hagoapp.f2t.*;
import com.hagoapp.f2t.csv.CsvTestConfig;
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer;
import com.hagoapp.f2t.datafile.FileTypeDeterminer;
import com.hagoapp.f2t.datafile.parquet.*;
import kotlin.Triple;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParquetFileTest {

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
                d.getRows().forEach(row -> {
                    for (int j = 0; j < row.getCells().size(); j++) {
                        var col = d.getColumnDefinition().get(j).getName();
                        var value = row.getCells().get(j);
                    }
                    var rowStr = IntStream.range(0, row.getCells().size()).mapToObj(i -> {
                        var col = d.getColumnDefinition().get(i).getName();
                        var value = row.getCells().get(i).getData();
                        return String.format("%s: %s", col, value);
                    }).collect(Collectors.joining(", "));
                    logger.info(rowStr);
                });
                Assertions.assertEquals(testConfig.getExpect().getColumnCount(), d.getColumnDefinition().size());
                //Assertions.assertEquals(testConfig.getExpect().getRowCount(), d.getRows().size());
            }
        }
    }

    @Test
    @Order(value = 3)
    public void testReadParquetColumnar() {
        Random random = new Random(Instant.now().getEpochSecond());
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var reader = new ParquetColumnarReader(config.getThird())) {
                var columns = reader.findColumns();
                logger.debug("columns: {}", columns);
                var pool = columns.stream().map(ParquetColumnarReader.ParquetColumn::getName)
                        .collect(Collectors.toList());
                var num = random.nextInt(pool.size() - 1);
                logger.debug("use {} columns", pool.size() - num);
                for (int i = num; i > 0; i--) {
                    int pos = random.nextInt(pool.size());
                    logger.debug("remove column {} {}", pos, pool.get(pos));
                    pool.remove(pos);
                }
                logger.debug("to read: {}", pool);
                var ret = reader.readColumns(pool, 0);
                logger.debug("read data: {}", ret);
            }
        }
    }

    @Test
    @Order(value = 4)
    public void testRepeatReadParquetColumnar() {
        Random random = new Random(Instant.now().getEpochSecond());
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var reader = new ParquetColumnarReader(config.getThird())) {
                var columns = reader.findColumns();
                logger.debug("columns: {}", columns);
                var pool = columns.stream().map(ParquetColumnarReader.ParquetColumn::getName)
                        .collect(Collectors.toList()).subList(0, 2);
                logger.debug("to read: {}", pool);
                var ret = reader.readColumns(pool);
                Assertions.assertEquals(2, ret.size());
                Assertions.assertEquals(108, ret.get("座次").size());
                Assertions.assertEquals(108, ret.get("星宿").size());
                logger.debug("read data: {}", ret);
                reader.reset();
                ret = reader.readColumns(pool, 2);
                Assertions.assertEquals("天魁星", ret.get("星宿").get(0));
                Assertions.assertEquals("天罡星", ret.get("星宿").get(1));
                Assertions.assertTrue(Long.valueOf(1).equals(ret.get("座次").get(0)) ||
                        Integer.valueOf(1).equals(ret.get("座次").get(0)));
                Assertions.assertTrue(Long.valueOf(2).equals(ret.get("座次").get(1)) ||
                        Integer.valueOf(2).equals(ret.get("座次").get(1)));
            }
        }
    }

    @Test
    @Order(value = 5)
    public void testReadParquetColumnarWithColumnNotExisted() {
        Random random = new Random(Instant.now().getEpochSecond());
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var reader = new ParquetColumnarReader(config.getThird())) {
                var columns = reader.findColumns();
                logger.debug("columns: {}", columns);
                var pool = columns.stream().map(ParquetColumnarReader.ParquetColumn::getName)
                        .collect(Collectors.toList());
                pool.set(random.nextInt(pool.size()), "ColumnNotExisted");
                logger.debug("to read: {}", pool);
                Assertions.assertThrows(IllegalArgumentException.class, () -> reader.readColumns(pool));
            }
        }
    }

    @Test
    @Order(value = 6)
    public void testMemoryParquetReader() throws IOException {
        Random random = new Random(Instant.now().getEpochSecond());
        logger.debug("test: {}", testConfigFiles);
        for (var config : testConfigFiles) {
            logger.debug("{}", config);
            try (var fis = new FileInputStream(config.getThird())) {
                var bytes = fis.readAllBytes();
                try (var ps = new MemoryParquetReader(bytes)) {
                    var columns = ps.getColumns();
                    var colCount = random.nextInt(columns.size());
                    var columnIndices = IntStream.range(0, columns.size()).boxed()
                            .collect(Collectors.toList());
                    var selectedColumns = IntStream.range(0, colCount).mapToObj(i -> {
                        int randomIndex = random.nextInt(columnIndices.size());
                        var elem = columns.get(columnIndices.get(randomIndex));
                        columnIndices.remove(randomIndex);
                        return elem;
                    }).collect(Collectors.toSet());
                    Predicate<String> columnSelector = selectedColumns::contains;
                    logger.info("test read 1 row with all columns");
                    var rows = ps.readRow();
                    Assertions.assertEquals(1, rows.size());
                    Assertions.assertEquals(columns.size(), rows.get(0).size());

                    var indexArray = IntStream.range(0, columns.size()).boxed()
                            .collect(Collectors.toList());
                    logger.info("test read 10 row with all columns");
                    rows = ps.readRow(10);
                    Assertions.assertEquals(10, rows.size());
                    Assertions.assertTrue(rows.stream().allMatch(r -> columns.size() == r.size()));
                    Assertions.assertTrue(rows.stream()
                            .allMatch(row -> indexArray.stream()
                                    .allMatch(i -> columns.get(i).equals(row.get(i).getFieldName()))));

                    logger.info("test read all remain row with all columns");
                    rows = ps.readRow(1000000);
                    Assertions.assertEquals(108 - 1 - 10, rows.size());
                    Assertions.assertTrue(rows.stream().allMatch(r -> columns.size() == r.size()));
                    Assertions.assertTrue(rows.stream()
                            .allMatch(row -> indexArray.stream()
                                    .allMatch(i -> columns.get(i).equals(row.get(i).getFieldName()))));

                }
            }
        }
    }
}
