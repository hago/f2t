/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.db;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.TableDefinition;
import com.hagoapp.f2t.TableUniqueDefinition;
import com.hagoapp.f2t.database.DbConnectionFactory;
import com.hagoapp.f2t.database.TableName;
import com.hagoapp.f2t.database.config.DbConfigReader;
import com.hagoapp.util.EncodingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

class DbConnectionTest {

    private final List<String> testConfigFiles = List.of(
            "tests/process/pgsql.sample.json",
            "tests/process/mariadb.sample.json",
            "tests/process/mssql.sample.json"
    );

    private final Logger logger = LoggerFactory.getLogger(DbConnectionTest.class);

    private static final String SKIP_TEST_DB_TYPE = "f2t.dbtests.skip";
    private static final List<String> skipped = new ArrayList<>();
    private static final List<ColumnDefinition> testColumnsDefinition = List.of(
            new ColumnDefinition("intCol", JDBCType.INTEGER),
            new ColumnDefinition("longCol", JDBCType.BIGINT),
            new ColumnDefinition("floatCol", JDBCType.FLOAT),
            new ColumnDefinition("doubleCol", JDBCType.DOUBLE),
            new ColumnDefinition("stringCol", JDBCType.CLOB),
            new ColumnDefinition("boolCol", JDBCType.BOOLEAN),
            new ColumnDefinition("timestampCol", JDBCType.TIMESTAMP)
    );

    @BeforeAll
    static void init() {
        if (System.getProperties().contains(SKIP_TEST_DB_TYPE)) {
            var parts = Arrays.stream(System.getProperty(SKIP_TEST_DB_TYPE).split(",")).map(String::trim)
                    .collect(Collectors.toList());
            skipped.addAll(parts);
        }
    }

    @Test
    void testDbConnection() throws SQLException, F2TException {
        for (var configFile : testConfigFiles) {
            if (skipped.contains(configFile)) {
                logger.debug("skip {}", configFile);
                continue;
            } else {
                logger.debug("testing using {}", configFile);
            }
            var config = DbConfigReader.readConfig(configFile);
            try (var conn = config.createConnection()) {
                try (var con = DbConnectionFactory.createDbConnection(conn)) {
                    var dbList = con.listDatabases();
                    Assertions.assertFalse(dbList.isEmpty());
                    var tables = con.getAvailableTables();
                    Assertions.assertFalse(tables.isEmpty());
                    var schema = tables.keySet().stream().findFirst().orElse(null);
                    Assertions.assertNotNull(schema);
                }
            }
        }
    }

    @Test
    void testWriteOperations() throws F2TException, SQLException {
        for (var configFile : testConfigFiles) {
            if (skipped.contains(configFile)) {
                logger.debug("skip {}", configFile);
                continue;
            } else {
                logger.debug("testing using {}", configFile);
            }
            var config = DbConfigReader.readConfig(configFile);
            try (var conn = config.createConnection()) {
                try (var con = DbConnectionFactory.createDbConnection(conn)) {
                    String testTable = "test_tbl";
                    var table = new TableName(testTable, con.getDefaultSchema());
                    // drop test table in case left by last failed tests.
                    con.dropTable(table);
                    var caseSensitive = con.isCaseSensitive();
                    var def = new TableDefinition<>(testColumnsDefinition, caseSensitive, null, false);
                    con.createTable(table, def);
                    var definition = con.getExistingTableDefinition(table);
                    Assertions.assertEquals(def.getColumns().size(), definition.getColumns().size());
                    var defCols = testColumnsDefinition.stream().map(ColumnDefinition::getName)
                            .collect(Collectors.toList());
                    Assertions.assertTrue(definition.getColumns().stream().map(ColumnDefinition::getName)
                            .allMatch(defCols::contains));
                    con.clearTable(table);
                    Assertions.assertTrue(con.isTableExists(table));
                    var r = con.dropTable(table);
                    logger.debug("drop message: {}", r.getSecond());
                    Assertions.assertTrue(r.getFirst());
                    Assertions.assertFalse(con.isTableExists(table));
                }
            }
        }
    }


    @Test
    void testNotExistedTable() throws F2TException, SQLException {
        var tbl = "t" + EncodingUtils.createRandomString(32, EncodingUtils.LETTERS_ONLY_CANDIDATE_CHARS);
        var schema = "s" + EncodingUtils.createRandomString(16, EncodingUtils.DEFAULT_CANDIDATE_CHARS);
        var table = new TableName(tbl, schema);
        for (var configFile : testConfigFiles) {
            if (skipped.contains(configFile)) {
                logger.debug("skip {}", configFile);
                continue;
            } else {
                logger.debug("testing using {}", configFile);
            }
            var config = DbConfigReader.readConfig(configFile);
            try (var conn = config.createConnection()) {
                try (var con = DbConnectionFactory.createDbConnection(conn)) {
                    Assertions.assertFalse(con.isTableExists(table));
                    Assertions.assertThrows(SQLException.class, () -> con.getExistingTableDefinition(table));
                    var r = con.clearTable(table);
                    Assertions.assertFalse(r.getFirst());
                    r = con.dropTable(table);
                    Assertions.assertFalse(r.getFirst());
                }
            }
        }
    }

    @Test
    void testCreateTable() throws F2TException, SQLException {
        for (var configFile : testConfigFiles) {
            if (skipped.contains(configFile)) {
                logger.debug("skip {}", configFile);
                continue;
            } else {
                logger.debug("testing using {}", configFile);
            }
            var config = DbConfigReader.readConfig(configFile);
            try (var conn = config.createConnection()) {
                try (var con = DbConnectionFactory.createDbConnection(conn)) {
                    var caseSensitive = con.isCaseSensitive();
                    var testTable = new TableName("test_tbl", con.getDefaultSchema());
                    var tableDefinitions = List.of(
                            createTableDefinition(caseSensitive),
                            createTableDefinition(caseSensitive, testColumnsDefinition.subList(0, 1).stream()
                                    .map(ColumnDefinition::getName).collect(Collectors.toList())),
                            createTableDefinition(
                                    caseSensitive,
                                    testColumnsDefinition.subList(0, 2).stream()
                                            .map(ColumnDefinition::getName).collect(Collectors.toList()),
                                    List.of(
                                            testColumnsDefinition.subList(2, 4).stream()
                                                    .map(ColumnDefinition::getName).collect(Collectors.toList()),
                                            testColumnsDefinition.subList(6, 7).stream()
                                                    .map(ColumnDefinition::getName).collect(Collectors.toList())
                                    )
                            )
                    );
                    for (var def : tableDefinitions) {
                        con.dropTable(testTable);
                        con.createTable(testTable, def);
                        Assertions.assertTrue(con.isTableExists(testTable));
                        con.dropTable(testTable);
                    }
                }
            }
        }
    }

    private TableDefinition<ColumnDefinition> createTableDefinition(boolean caseSensitive) {
        return new TableDefinition<>(testColumnsDefinition, caseSensitive, null, false);
    }

    private TableDefinition<ColumnDefinition> createTableDefinition(boolean caseSensitive, List<String> primaryKeyColumns) {
        return createTableDefinition(caseSensitive, primaryKeyColumns, null);
    }

    private TableDefinition<ColumnDefinition> createTableDefinition(
            boolean caseSensitive, List<String> primaryKeyColumns, List<List<String>> uniques) {
        BiFunction<String, String, Boolean> colMatcher = caseSensitive ? String::equals : String::equalsIgnoreCase;
        var primaryKey = new TableUniqueDefinition<>("primaryKey_unit_test", primaryKeyColumns.stream()
                .map(colName ->
                        testColumnsDefinition.stream().filter(col -> colMatcher.apply(col.getName(), colName))
                                .findFirst().orElse(null)
                ).collect(Collectors.toList()), caseSensitive);
        primaryKey.getColumns().forEach(col -> col.getTypeModifier().setNullable(false));
        var tblDef = new TableDefinition<>(testColumnsDefinition, caseSensitive, primaryKey, false);
        if (uniques != null) {
            var uniqueDef = uniques.stream().map(unique -> {
                var uniqueCols = unique.stream().map(colName ->
                        testColumnsDefinition.stream().filter(col -> colMatcher.apply(col.getName(), colName))
                                .findFirst().orElse(null)
                ).collect(Collectors.toList());
                var name = "unique_" + String.join("_", unique);
                return new TableUniqueDefinition<>(name, uniqueCols, caseSensitive);
            }).collect(Collectors.toSet());
            tblDef.setUniqueConstraints(uniqueDef);
        }
        return tblDef;
    }
}