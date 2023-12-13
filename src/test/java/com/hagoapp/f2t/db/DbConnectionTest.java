/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.db;

import com.hagoapp.f2t.database.DbConnectionFactory;
import com.hagoapp.f2t.database.TableName;
import com.hagoapp.f2t.database.config.DbConfigReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @BeforeAll
    static void init() {
        if (System.getProperties().contains(SKIP_TEST_DB_TYPE)) {
            var parts = Arrays.stream(System.getProperty(SKIP_TEST_DB_TYPE).split(",")).map(String::trim)
                    .collect(Collectors.toList());
            skipped.addAll(parts);
        }
    }

    @Test
    void testDbConnection() {
        for (var configFile : testConfigFiles) {
            if (skipped.contains(configFile)) {
                logger.debug("skip {}", configFile);
                continue;
            } else {
                logger.debug("testing using {}", configFile);
            }
            Assertions.assertDoesNotThrow(() -> {
                var config = DbConfigReader.readConfig(configFile);
                try (var conn = config.createConnection()) {
                    try (var con = DbConnectionFactory.createDbConnection(conn)) {
                        var tables = con.getAvailableTables();
                        Assertions.assertFalse(tables.isEmpty());
                        var schema = tables.entrySet().stream().filter(i -> !i.getValue().isEmpty())
                                .findFirst()
                                .map(Map.Entry::getKey)
                                .orElse(null);
                        Assertions.assertNotNull(schema);
                        var schemaTables = tables.get(schema);
                        Assertions.assertFalse(schemaTables.isEmpty());
                        var table = schemaTables.get(0);
                        Assertions.assertTrue(con.isTableExists(table));
                        Assertions.assertFalse(con.isTableExists(new TableName("NotFound", "404")));
                        con.getExistingTableDefinition(table);
                    }
                }
            });
        }
    }
}
