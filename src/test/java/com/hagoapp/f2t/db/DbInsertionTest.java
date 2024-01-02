/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.db;

import com.hagoapp.f2t.*;
import com.hagoapp.f2t.database.DbConnectionFactory;
import com.hagoapp.f2t.database.TableName;
import com.hagoapp.f2t.database.config.DbConfigReader;
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer;
import com.hagoapp.f2t.datafile.FileTypeDeterminer;
import com.hagoapp.f2t.datafile.csv.CSVDataReader;
import com.hagoapp.f2t.datafile.csv.FileInfoCsv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DbInsertionTest {
    private final List<String> testConfigFiles = List.of(
            "tests/process/pgsql.sample.json",
            "tests/process/mariadb.sample.json",
            "tests/process/mssql.sample.json"
    );

    private final Logger logger = LoggerFactory.getLogger(DbConnectionTest.class);

    private static final String SKIP_TEST_DB_TYPE = "f2t.dbtests.skip";
    private static final String TEST_FILE = "tests/csv/shuihudata.csv";
    private static final String TEST_TABLE_NAME = "test_INSERT_Shuihu_data";
    private static final List<String> skipped = new ArrayList<>();
    private static List<FileColumnDefinition> fileColumnDefinitions;
    private static final List<DataRow> rows = new ArrayList<>();

    @BeforeAll
    public static void init() {
        if (System.getProperties().contains(SKIP_TEST_DB_TYPE)) {
            var parts = Arrays.stream(System.getProperty(SKIP_TEST_DB_TYPE).split(",")).map(String::trim)
                    .collect(Collectors.toList());
            skipped.addAll(parts);
        }
        var info = new FileInfoCsv();
        info.setFilename(TEST_FILE);
        try (var reader = new CSVDataReader()) {
            reader.open(info);
            reader.setupTypeDeterminer(new FileTypeDeterminer(FileColumnTypeDeterminer.getMostTypeDeterminer()));
            fileColumnDefinitions = reader.findColumns();
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        }
    }

    @Test
    void testWriteRows() throws F2TException, SQLException {
        var configFiles = testConfigFiles.stream().filter(tf -> !skipped.contains(tf)).collect(Collectors.toList());
        for (var cfgFile : configFiles) {
            var cfg = DbConfigReader.readConfig(cfgFile);
            try (var conn = cfg.createConnection()) {
                try (var con = DbConnectionFactory.createDbConnection(conn)) {
                    var testTable = new TableName(TEST_TABLE_NAME, con.getDefaultSchema());
                    con.dropTable(testTable);
                    var tableDef = new TableDefinition<>(
                            fileColumnDefinitions.stream().map(fileCol -> {
                                var dbCol = new ColumnDefinition(
                                        fileCol.getName(), fileCol.getDataType()
                                );
                                dbCol.setTypeModifier(fileCol.getTypeModifier());
                                return dbCol;
                            }).collect(Collectors.toList()),
                            con.isCaseSensitive(), null, false
                    );
                    con.createTable(testTable, tableDef);
                    var fileDefinition = new TableDefinition<>(fileColumnDefinitions, true, null, false);
                    con.prepareInsertion(fileDefinition, testTable, tableDef);
                    long i = 0;
                    var fullName = con.getFullTableName(testTable);
                    for (var row : rows) {
                        con.writeRow(testTable, row);
                        i++;
                        logger.debug("{} rows written into {}", i, fullName);
                    }
                    con.flushRows(testTable);
                    var size = con.queryTableSize(testTable);
                    logger.debug("queried size of {} is {}", fullName, size);
                    Assertions.assertEquals(rows.size(), size);
                    con.dropTable(testTable);
                }
            }
        }
    }
}
