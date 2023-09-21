/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.db;

import com.hagoapp.f2t.ColumnDefinition;
import com.hagoapp.f2t.Constants;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.database.DbConnectionFactory;
import com.hagoapp.f2t.database.TableName;
import com.hagoapp.f2t.database.config.DbConfigReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.SQLException;
import java.util.ArrayList;

@EnabledIfSystemProperty(named = Constants.DATABASE_CONFIG_FILE, matches = ".*")
class TableDataTest {
    @Test
    @EnabledIfSystemProperties({
            @EnabledIfSystemProperty(named = Constants.DATABASE_TEST_SCHEMA, matches = ".*"),
            @EnabledIfSystemProperty(named = Constants.DATABASE_TEST_TABLE, matches = ".*")
    })
    void testReadData() {
        Assertions.assertDoesNotThrow(() -> {
            var config = DbConfigReader.readConfig(System.getProperty(Constants.DATABASE_CONFIG_FILE));
            try (var sqlCon = config.createConnection()) {
                try (var connection = DbConnectionFactory.createDbConnection(sqlCon, config.getProperties())) {
                    var schema = System.getProperty(Constants.DATABASE_TEST_SCHEMA);
                    var table = System.getProperty(Constants.DATABASE_TEST_TABLE);
                    var def = connection.getExistingTableDefinition(new TableName(table, schema));
                    var rows = connection.readData(new TableName(table, schema),
                            new ArrayList<ColumnDefinition>(def.getColumns()), 10);
                    System.out.print(rows);
                }
            }
        });
    }
}
