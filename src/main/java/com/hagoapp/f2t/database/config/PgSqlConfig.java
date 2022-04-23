/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration for PostgreSQL database.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
public class PgSqlConfig extends DbConfig {
    public static final String DATABASE_TYPE_POSTGRESQL = "PostgreSQL";
    private String host;
    private int port = 5432;

    public PgSqlConfig() {
        super();
        this.dbType = DATABASE_TYPE_POSTGRESQL;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public Connection createConnection() throws SQLException {
        var db = ((databaseName == null) || databaseName.isBlank()) ? "postgres" : databaseName;
        if ((host == null) || (username == null) || (password == null)) {
            throw new UnsupportedOperationException("Configuration is incomplete");
        }
        var conStr = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);
        var props = new Properties();
        props.putAll(Map.of("user", username, "password", password));
        return DriverManager.getConnection(conStr, props);
    }
}
