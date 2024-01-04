/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.DataCell;
import com.hagoapp.f2t.DataRow;
import com.hagoapp.f2t.TableDefinition;
import com.hagoapp.f2t.datafile.parquet.MemoryParquetReader;
import com.hagoapp.f2t.datafile.parquet.ParquetIteratorWriter;
import com.hagoapp.f2t.datafile.parquet.ParquetWriterConfig;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.JDBCType;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LargeParquetTest {
    private static final Logger logger = LoggerFactory.getLogger(LargeParquetTest.class);
    private static final String LARGE_PARQUET_TEST_FILE = "f2t.parquet.file.2g";
    private static final String WORDS_POOL_FILE = "tests/parquet/shuihupiece.txt";
    private static final String TEST_LARGE_PARQUET_FILE = "large.parquet";
    private static final String WRITE_HUGE_PARQUET = "f2t.parquet.huge.create";
    private static final String WRITE_CUSTOM_ROW_COUNT = "f2t.parquet.row.count";
    private static final long HUGE_PARQUET_ROW_COUNT = 1400000L;
    private static final long DEFAULT_PARQUET_ROW_COUNT = 200L;
    private static String testParquetFile;

    private static final TableDefinition<ColumnDefinition> TEST_PARQUET_SCHEMA = new TableDefinition<>(
            List.of(
                    new ColumnDefinition("numberint", JDBCType.INTEGER),
                    new ColumnDefinition("numberlong", JDBCType.BIGINT),
                    new ColumnDefinition("text", JDBCType.CLOB),
                    new ColumnDefinition("timestamp", JDBCType.BIGINT),
                    new ColumnDefinition("datetime", JDBCType.TIMESTAMP),
                    new ColumnDefinition("charseq", JDBCType.VARCHAR),
                    new ColumnDefinition("numberdouble", JDBCType.DOUBLE)
            ),
            true, null, false
    );

    @BeforeAll
    public static void init() {
        var r = System.getProperty(WRITE_CUSTOM_ROW_COUNT);
        long rowCount;
        if (r == null) {
            rowCount = System.getProperty(WRITE_HUGE_PARQUET) == null ?
                    DEFAULT_PARQUET_ROW_COUNT : HUGE_PARQUET_ROW_COUNT;
        } else {
            rowCount = Long.parseLong(r);
        }
        testParquetFile = rowCount == HUGE_PARQUET_ROW_COUNT ? TEST_LARGE_PARQUET_FILE
                : String.format("test_file_%d.parquet", rowCount);
    }

    @AfterAll
    public static void clean() {
        deleteLargeParquet();
    }

    private static void deleteLargeParquet() {
        try {
            Files.delete(Path.of(testParquetFile));
        } catch (IOException ignored) {
            //
        }
    }

    @Test
    @EnabledIfSystemProperty(named = LARGE_PARQUET_TEST_FILE, matches = ".*?")
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

    private static class Feeder implements Iterator<DataRow> {
        private long count = 0L;
        private final long rowAmount;
        private final SecureRandom rand = new SecureRandom();
        private final List<ColumnDefinition> cols = TEST_PARQUET_SCHEMA.getColumns();

        private final String words;

        public Feeder(String words, long rowAmount) {
            this.words = words;
            this.rowAmount = rowAmount;
        }

        @Override
        public boolean hasNext() {
            return count < rowAmount;
        }

        @Override
        public DataRow next() {
            Object[] data = new Object[]{
                    rand.nextInt(),
                    rand.nextLong(),
                    randomWords(1000),
                    Instant.now().toEpochMilli(),
                    ZonedDateTime.now(),
                    randomWords(20),
                    rand.nextDouble()
            };
            var row = new DataRow(count, IntStream.range(0, cols.size())
                    .mapToObj(i -> new DataCell(data[i], i))
                    .collect(Collectors.toList()));
            count++;
            return row;
        }

        private String randomWords(int maxLength) {
            var start = rand.nextInt(words.length());
            var len = rand.nextInt(Math.min(maxLength, words.length() - start));
            return words.substring(start, start + len);
        }
    }

    @Test
    @Order(1)
    void testWriteHugeParquet() throws IOException {
        var r = System.getProperty(WRITE_CUSTOM_ROW_COUNT);
        long rowCount;
        if (r == null) {
            rowCount = System.getProperty(WRITE_HUGE_PARQUET) == null ?
                    DEFAULT_PARQUET_ROW_COUNT : HUGE_PARQUET_ROW_COUNT;
        } else {
            rowCount = Long.parseLong(r);
        }
        testParquetFile = rowCount == HUGE_PARQUET_ROW_COUNT ? TEST_LARGE_PARQUET_FILE
                : String.format("test_file_%d.parquet", rowCount);
        logger.debug("creating parquet file {} with {} rows", testParquetFile, rowCount);
        String words;
        try (var fis = new FileInputStream(WORDS_POOL_FILE)) {
            words = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
        }
        var feeder = new Feeder(words, rowCount);
        var config = new ParquetWriterConfig("com.hagoapp.f2t", "test", testParquetFile);
        try (var writer = ParquetIteratorWriter.createWriter(TEST_PARQUET_SCHEMA, feeder, config)) {
            writer.write();
        }
        Assertions.assertTrue(new File(testParquetFile).exists());
    }

    @Test
    @Order(2)
    void testReadParquet() throws IOException {
        String testFile;
        var largeFile = new File(TEST_LARGE_PARQUET_FILE);
        if (!largeFile.exists()) {
            logger.warn("Large parquet file {} not existed, skip", TEST_LARGE_PARQUET_FILE);
            testFile = testParquetFile;
        } else {
            testFile = TEST_LARGE_PARQUET_FILE;
        }
        try (var fis = new FileInputStream(testFile)) {
            try (var reader = MemoryParquetReader.create(fis)) {
                var cols = reader.getColumns();
                Assertions.assertFalse(cols.isEmpty());
                reader.skip(1);
                var data = reader.read(10);
            }
        }
    }
}
