/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.DataCell;
import com.hagoapp.f2t.DataRow;
import com.hagoapp.f2t.DataTable;
import com.hagoapp.f2t.datafile.parquet.ParquetWriter;
import com.hagoapp.f2t.datafile.parquet.ParquetWriterConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.JDBCType;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReadWriteTest {

    private final static String test_parquet_name = "test.parquet";
    private static final Logger logger = LoggerFactory.getLogger(ReadWriteTest.class);
    private static ParquetWriterConfig writeConfig;
    private static DataTable<ColumnDefinition> dataTable;

    private static final List<ColumnDefinition> columnDefinitions = List.of(
            createColumnDefinition("id", JDBCType.INTEGER),
            createColumnDefinition("longId", JDBCType.BIGINT),
            createColumnDefinition("name", JDBCType.CLOB),
            createColumnDefinition("bool", JDBCType.BOOLEAN),
            createColumnDefinition("float", JDBCType.FLOAT),
            createColumnDefinition("double", JDBCType.DOUBLE),
            createColumnDefinition("timestamp", JDBCType.TIMESTAMP),
            createColumnDefinition("timestamp_zone", JDBCType.TIMESTAMP_WITH_TIMEZONE),
            createColumnDefinition("date", JDBCType.DATE),
            createColumnDefinition("time", JDBCType.TIME),
            createColumnDefinition("time_zone", JDBCType.TIME_WITH_TIMEZONE),
            createColumnDefinition("bin", JDBCType.VARBINARY)
    );

    private static final List<DataRow> rows = List.of(
            createRow(List.of(1, 1L, "John", false, 0.1f, 2.67709876543, LocalDateTime.now(), ZonedDateTime.now(), LocalDate.now(), LocalTime.now(), Time.valueOf("13:01:02"), new byte[]{0, 1, 0}))
            //createRow(IntStream.range(0, columnDefinitions.size()).mapToObj(i -> null).collect(Collectors.toList()))
    );

    @BeforeAll
    static void init() {
        writeConfig = new ParquetWriterConfig("com.lenovo.test", "parquet.test", test_parquet_name);
        dataTable = new DataTable<>(columnDefinitions, rows);
    }

    private static ColumnDefinition createColumnDefinition(String name, JDBCType type) {
        var col = new ColumnDefinition();
        col.setName(name);
        col.setDataType(type);
        return col;
    }

    private static long rowNo = 0;

    private static DataRow createRow(List<Object> data) {
        var cells = IntStream.range(0, data.size()).mapToObj(i -> {
            var cell = new DataCell();
            cell.setIndex(i);
            cell.setData(data.get(i));
            return cell;
        }).collect(Collectors.toList());
        var row = new DataRow(rowNo, cells);
        rowNo++;
        return row;
    }

    @AfterAll
    static void cleanUp() {
        try {
            //var r = new File(test_parquet_name).delete();
        } catch (SecurityException e) {
            logger.error("Can't write file {}: {}", test_parquet_name, e.getMessage());
        }
    }

    @Test
    @Order(value = 1)
    public void testWrite() {
        var writer = new ParquetWriter(dataTable, writeConfig);
        writer.write();
    }

    @Test
    @Order(value = 2)
    public void testRead() {

    }
}
