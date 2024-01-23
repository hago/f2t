/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SqliteConfig extends DbConfig {

    public static final String SQLITE_DRIVER_NAME = "org.sqlite.jdbc";

    private boolean useMemoryDb = false;

    public boolean isUseMemoryDb() {
        return useMemoryDb;
    }

    public void setUseMemoryDb(boolean useMemoryDb) {
        this.useMemoryDb = useMemoryDb;
    }

    @Override
    public String getDriverName() {
        return SQLITE_DRIVER_NAME;
    }

    @Override
    public String getDbType() {
        return "SQLite";
    }

    @Override
    public Connection createConnection() throws SQLException {
        var conStr = String.format("jdbc:sqlite:%s", isUseMemoryDb() ? "" : getDatabaseName());
        return DriverManager.getConnection(conStr);
    }
}
