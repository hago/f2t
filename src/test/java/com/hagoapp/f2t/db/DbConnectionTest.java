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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class DbConnectionTest {

    private final List<String> testConfigFiles = List.of(
            "tests/process/pgsql.sample.json",
            "tests/process/mariadb.sample.json",
            "tests/process/mssql.sample.json",
            "tests/process/derby.sample.json",
            "tests/process/sqlite.sample.json"
    );

    private final Logger logger = LoggerFactory.getLogger(DbConnectionTest.class);

    private static final String SKIP_TEST_DB_TYPE = "f2t.dbtests.skip";
    private static final String SELECT_TEST_DB_TYPE = "f2t.dbtests.select";
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

    private void initDerbyDemoTable() throws F2TException, SQLException, IOException {
        var derbyConfig = testConfigFiles.stream().filter(fn -> fn.contains("derby")).findFirst().orElse(null);
        if (derbyConfig != null) {
            var cfg = DbConfigReader.readConfig(derbyConfig);
            String sql;
            try (var fis = new FileInputStream("tests/sql/derby.sql")) {
                sql = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
            }
            try (var con = cfg.createConnection()) {
                try (var conn = DbConnectionFactory.createDbConnection(con)) {
                    if (conn.isTableExists(new TableName("demo", conn.getDefaultSchema()))) {
                        return;
                    }
                }
                try (var st = con.prepareStatement(sql)) {
                    logger.debug("create demo table for Derby");
                    st.execute();
                }
            }
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
                    logger.debug("tables: {}", tables);
                    Assertions.assertFalse(tables.isEmpty());
                    var schema = tables.keySet().stream().findFirst().orElse(null);
                    Assertions.assertNotNull(schema);
                }
            }
        }
    }

    /**
     * A demo table following from SQL files in tests/sql directory should be created manually before running this test.
     *
     * @throws F2TException if DbConnection instance methods fail
     * @throws SQLException if creating java.sql.connection fails
     */
    @Test
    void testTableDefinition() throws F2TException, SQLException, IOException {
        initDerbyDemoTable();
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
                    var tbl = new TableName("demo", con.getDefaultSchema());
                    var def = con.getExistingTableDefinition(tbl);
                    Assertions.assertFalse(def.getColumns().isEmpty());
                    Assertions.assertNotNull(def.getPrimaryKey());
                    var uniques = def.getUniqueConstraints();
                    Assertions.assertEquals(2, uniques.size());
                    var nameAgeUnique = uniques.stream().filter(u -> u.getColumns().size() == 2)
                            .findFirst().orElse(null);
                    var numberAgeLongUnique = uniques.stream().filter(u -> u.getColumns().size() == 3)
                            .findFirst().orElse(null);
                    Assertions.assertNotNull(nameAgeUnique);
                    Assertions.assertNotNull(numberAgeLongUnique);
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
                    var tableDefinitions = createTableDefinition(caseSensitive);
                    con.dropTable(testTable);
                    con.createTable(testTable, tableDefinitions);
                    Assertions.assertTrue(con.isTableExists(testTable));
                    var def = con.getExistingTableDefinition(testTable);
                    Assertions.assertTrue(areTableColumnNamesEqual(tableDefinitions, def, caseSensitive));
                    Assertions.assertNull(def.getPrimaryKey());
                    Assertions.assertTrue(def.getUniqueConstraints().isEmpty());
                    con.dropTable(testTable);
                }
            }
        }
    }

    @Test
    void testCreateTableWithPrimaryKey() throws F2TException, SQLException {
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
                    var tableDefinitions = createTableDefinition(caseSensitive, testColumnsDefinition.subList(0, 1).stream()
                            .map(ColumnDefinition::getName).collect(Collectors.toList()));
                    con.dropTable(testTable);
                    con.createTable(testTable, tableDefinitions);
                    Assertions.assertTrue(con.isTableExists(testTable));
                    var def = con.getExistingTableDefinition(testTable);
                    Assertions.assertTrue(areTableColumnNamesEqual(tableDefinitions, def, caseSensitive));
                    Assertions.assertNotNull(def.getPrimaryKey());
                    Assertions.assertNotNull(def.getPrimaryKey().getColumns());
                    Assertions.assertTrue(isPrimaryKeyEqual(tableDefinitions, def, caseSensitive));
                    Assertions.assertTrue(def.getUniqueConstraints().isEmpty());
                    con.dropTable(testTable);
                }
            }
        }
    }

    @Test
    void testCreateTableWithPrimaryKeyAndUnique() throws F2TException, SQLException {
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
                    var tableDefinitions = createTableDefinition(
                            caseSensitive,
                            testColumnsDefinition.subList(0, 2).stream()
                                    .map(ColumnDefinition::getName).collect(Collectors.toList()),
                            List.of(
                                    testColumnsDefinition.subList(2, 4).stream()
                                            .map(ColumnDefinition::getName).collect(Collectors.toList()),
                                    testColumnsDefinition.subList(6, 7).stream()
                                            .map(ColumnDefinition::getName).collect(Collectors.toList())
                            )
                    );
                    con.dropTable(testTable);
                    con.createTable(testTable, tableDefinitions);
                    Assertions.assertTrue(con.isTableExists(testTable));
                    var def = con.getExistingTableDefinition(testTable);
                    Assertions.assertTrue(areTableColumnNamesEqual(tableDefinitions, def, caseSensitive));
                    Assertions.assertNotNull(def.getPrimaryKey());
                    Assertions.assertNotNull(def.getPrimaryKey().getColumns());
                    Assertions.assertTrue(isPrimaryKeyEqual(tableDefinitions, def, caseSensitive));
                    Assertions.assertFalse(def.getUniqueConstraints().isEmpty());
                    Assertions.assertEquals(tableDefinitions.getUniqueConstraints().size(), def.getUniqueConstraints().size());
                    Assertions.assertTrue(areUniqueConstraintsEqual(
                            tableDefinitions.getUniqueConstraints(), def.getUniqueConstraints()));
                    con.dropTable(testTable);
                }
            }
        }
    }

    private boolean areTableColumnNamesEqual(
            TableDefinition<? extends ColumnDefinition> t1,
            TableDefinition<? extends ColumnDefinition> t2, boolean caseSensitive) {
        var cols1 = t1.getColumns();
        var cols2 = t2.getColumns();
        return areColumnsEqual(cols1, cols2, caseSensitive);
    }

    private boolean areColumnsEqual(
            List<? extends ColumnDefinition> cols1,
            List<? extends ColumnDefinition> cols2, boolean caseSensitive) {
        BiFunction<String, String, Boolean> colMatcher = caseSensitive ? String::equals : String::equalsIgnoreCase;
        return cols1.size() == cols2.size() &&
                IntStream.range(0, cols1.size()).allMatch(i -> colMatcher.apply(cols1.get(i).getName(), cols2.get(i).getName()));
    }

    private boolean isPrimaryKeyEqual(
            TableDefinition<? extends ColumnDefinition> t1,
            TableDefinition<? extends ColumnDefinition> t2, boolean caseSensitive) {
        var p1 = t1.getPrimaryKey();
        var p2 = t2.getPrimaryKey();
        if ((p1 == null) && (p2 == null)) {
            return true;
        } else if ((p1 == null) || (p2 == null)) {
            return false;
        } else {
            return areColumnsEqual(p1.getColumns(), p2.getColumns(), caseSensitive);
        }
    }

    private boolean areUniqueConstraintsEqual(
            Set<TableUniqueDefinition<ColumnDefinition>> ul1,
            Set<? extends TableUniqueDefinition<? super ColumnDefinition>> ul2
    ) {
        return ul1.stream().allMatch(u1 ->
                ul2.stream().anyMatch(u2 -> (u1.getCaseSensitive() == u2.getCaseSensitive()) && u2.compareColumns(u1))
        );
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
