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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperties;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.stream.Collectors;

@EnabledIfSystemProperty(named = Constants.DATABASE_CONFIG_FILE, matches = ".*")
public class TableDataTest {
    @Test
    @EnabledIfSystemProperties({
            @EnabledIfSystemProperty(named = Constants.DATABASE_TEST_SCHEMA, matches = ".*"),
            @EnabledIfSystemProperty(named = Constants.DATABASE_TEST_TABLE, matches = ".*")
    })
    public void testReadData() throws F2TException {
        var config = DbConfigReader.readConfig(System.getProperty(Constants.DATABASE_CONFIG_FILE));
        var connection = DbConnectionFactory.createDbConnection(config);
        var schema = System.getProperty(Constants.DATABASE_TEST_SCHEMA);
        var table = System.getProperty(Constants.DATABASE_TEST_TABLE);
        var def = connection.getExistingTableDefinition(new TableName(table, schema));
        var rows = connection.readData(new TableName(table, schema),
                def.getColumns().stream().collect(Collectors.toList()), 10);
        System.out.print(rows);
    }
}
