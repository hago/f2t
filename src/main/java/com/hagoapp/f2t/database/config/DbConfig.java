/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import com.hagoapp.f2t.JsonStringify;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * The basic configuration for database connections.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
public class DbConfig implements JsonStringify {
    protected String dbType;
    protected String username;
    protected String password;
    protected String databaseName;

    public String getDriverName() {
        throw new UnsupportedOperationException("Not implemented for the DbConfig base class");
    }

    /**
     * Database type or brand name, an identifier for a specified database.
     *
     * @return database type string
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * Database username.
     *
     * @return username
     */
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Database password.
     *
     * @return password
     */
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Database name.
     *
     * @return database name
     */
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Connection createConnection() throws SQLException {
        throw new UnsupportedOperationException("The base class have no enough information to create connection!");
    }

    public Map<String, Object> getProperties() {
        return Map.of();
    }
}
