/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

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

import java.sql.SQLException;
import java.util.Set;

public class TableDefinitionTest {

    private static final String DEFINITION_SAMPLE_FROM_FILE = "tests/csv/shuihudata.csv";
    private static TableDefinition<FileColumnDefinition> fileTableDefinition;
    private static final String DEFINITION_SAMPLE_FROM_DB_CONFIG = "tests/process/pgsql.sample.json";
    private static final String DEFINITION_SAMPLE_FROM_TABLE = "demo";
    private static TableDefinition<? extends ColumnDefinition> dbTableDefinition;

    @BeforeAll
    public static void init() throws F2TException, SQLException {
        var info = new FileInfoCsv();
        info.setFilename(DEFINITION_SAMPLE_FROM_FILE);
        try (var reader = new CSVDataReader()) {
            reader.open(info);
            reader.setupTypeDeterminer(new FileTypeDeterminer(FileColumnTypeDeterminer.getMostTypeDeterminer()));
            var cols = reader.inferColumnTypes(-1);
            fileTableDefinition = new TableDefinition<>(cols, true, null, false);
        }
        dbTableDefinition = loadDbTableDefinition();
    }

    private static TableDefinition<? extends ColumnDefinition> loadDbTableDefinition() throws F2TException, SQLException {
        var cfg = DbConfigReader.readConfig(DEFINITION_SAMPLE_FROM_DB_CONFIG);
        try (var con = cfg.createConnection()) {
            try (var conn = DbConnectionFactory.createDbConnection(con)) {
                var tbl = new TableName(DEFINITION_SAMPLE_FROM_TABLE, conn.getDefaultSchema());
                return conn.getExistingTableDefinition(tbl);
            }
        }
    }

    @Test
    void testEquals() throws F2TException, SQLException {
        Assertions.assertNotEquals(dbTableDefinition, fileTableDefinition);
        var dbTableDefinition2 = loadDbTableDefinition();
        Assertions.assertEquals(dbTableDefinition, dbTableDefinition2);
        dbTableDefinition2.getColumns().remove(0);
        Assertions.assertNotEquals(dbTableDefinition, dbTableDefinition2);

        dbTableDefinition2 = loadDbTableDefinition();
        dbTableDefinition2.setCaseSensitive(!dbTableDefinition2.getCaseSensitive());
        Assertions.assertNotEquals(dbTableDefinition, dbTableDefinition2);

        dbTableDefinition2 = loadDbTableDefinition();
        dbTableDefinition2.setPrimaryKey(null);
        Assertions.assertNotEquals(dbTableDefinition, dbTableDefinition2);

        dbTableDefinition2 = loadDbTableDefinition();
        dbTableDefinition2.setUniqueConstraints(Set.of());
        Assertions.assertNotEquals(dbTableDefinition, dbTableDefinition2);
    }

    @Test
    void testToString() {
        Assertions.assertDoesNotThrow(() -> dbTableDefinition.toString());
    }

    @Test
    void testHashCode() {
        Assertions.assertDoesNotThrow(() -> dbTableDefinition.hashCode());
    }
}
