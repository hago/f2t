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
 * Configuration for Microsoft SQl Server database, and may be used for Azure SQL DB and Synapse DW.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
public class MsSqlConfig extends DbConfig {

    public static final String DATABASE_TYPE_MSSQL = "Microsoft SQL Sever";

    public MsSqlConfig() {
        super();
        dbType = DATABASE_TYPE_MSSQL;
    }

    private String host;
    private int port = 1433;
    private boolean trustServerCertificate = true;

    public boolean isTrustServerCertificate() {
        return trustServerCertificate;
    }

    public void setTrustServerCertificate(boolean trustServerCertificate) {
        this.trustServerCertificate = trustServerCertificate;
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
    public String getDriverName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public Connection createConnection() throws SQLException {
        var db = ((databaseName == null) || databaseName.isBlank()) ? "master" : databaseName;
        if ((host == null) || (username == null) || (password == null)) {
            throw new UnsupportedOperationException("Configuration is incomplete");
        }
        String conStr = String.format("jdbc:sqlserver://%s:%d;databaseName = %s", host, port, db) +
                ";trustServerCertificate=" + trustServerCertificate;
        var props = new Properties();
        props.putAll(Map.of("user", username, "password", password));
        return DriverManager.getConnection(conStr, props);
    }
}
