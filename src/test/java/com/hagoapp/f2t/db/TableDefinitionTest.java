/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.db;

import com.hagoapp.f2t.Constants;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.database.DbConnectionFactory;
import com.hagoapp.f2t.database.TableName;
import com.hagoapp.f2t.database.config.DbConfigReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

@EnabledIfSystemProperty(named = Constants.DATABASE_CONFIG_FILE, matches = ".*")
public class TableDefinitionTest {

    private final Logger logger = LoggerFactory.getLogger(TableDefinitionTest.class);

    @Test
    @EnabledIfSystemProperties({
            @EnabledIfSystemProperty(named = Constants.DATABASE_TEST_SCHEMA, matches = ".*"),
            @EnabledIfSystemProperty(named = Constants.DATABASE_TEST_TABLE, matches = ".*")
    })
    public void testFindTableDefinition() throws F2TException, SQLException {
        var config = DbConfigReader.readConfig(System.getProperty(Constants.DATABASE_CONFIG_FILE));
        try (var sqlCon = config.createConnection()) {
            try (var connection = DbConnectionFactory.createDbConnection(sqlCon, config.getProperties())) {
                var schema = System.getProperty(Constants.DATABASE_TEST_SCHEMA);
                var table = System.getProperty(Constants.DATABASE_TEST_TABLE);
                var def = connection.getExistingTableDefinition(new TableName(table, schema));
                logger.debug("def: {}", def);
                Assertions.assertFalse(def.getColumns().isEmpty());
                def.getColumns().forEach(colDef -> {
                    Assertions.assertNotNull(colDef.getDataType());
                    switch (colDef.getDataType()) {
                        case CHAR:
                        case NCHAR:
                        case VARCHAR:
                        case NVARCHAR:
                        case LONGNVARCHAR:
                        case BINARY:
                        case VARBINARY:
                            Assertions.assertTrue(colDef.getTypeModifier().getMaxLength() > 0);
                            break;
                        case FLOAT:
                        case DOUBLE:
                        case NUMERIC:
                        case DECIMAL:
                            break;
                    }
                });
            }
        }
    }

    @Test
    public void testFindTables() throws F2TException, SQLException {
        var config = DbConfigReader.readConfig(System.getProperty(Constants.DATABASE_CONFIG_FILE));
        try (var sqlCon = config.createConnection()) {
            try (var connection = DbConnectionFactory.createDbConnection(sqlCon, config.getProperties())) {
                connection.getAvailableTables().forEach((schema, tableNames) -> {
                    logger.debug("Found schema: {}", schema);
                    tableNames.forEach(tableName ->
                            logger.debug("Found table: '{}'", connection.getFullTableName(tableName)));
                });
            }
        }
    }

}
