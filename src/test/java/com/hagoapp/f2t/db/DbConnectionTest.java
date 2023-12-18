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
import java.util.Map;
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
            new ColumnDefinition("intString", JDBCType.CLOB),
            new ColumnDefinition("intBool", JDBCType.BOOLEAN),
            new ColumnDefinition("intTimestamp", JDBCType.TIMESTAMP)
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
                    var primaryKeyDef = new TableUniqueDefinition<>(
                            "pkey_unit_test",
                            testColumnsDefinition.subList(0, 1),
                            caseSensitive
                    );
                    var def = new TableDefinition<>(
                            testColumnsDefinition,
                            caseSensitive,
                            primaryKeyDef,
                            false
                    );
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
}
