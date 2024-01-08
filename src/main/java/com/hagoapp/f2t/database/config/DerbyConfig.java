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

public class DerbyConfig extends DbConfig {

    private static final String DATABASE_TYPE_APACHE_DERBY = "Apache Derby";
    public static final String JDBC_DRIVER_APACHE_DERBY = "org.apache.derby.iapi.jdbc.AutoLoadedDriver";
    private boolean create = true;
    private String bootPassword;

    public boolean isCreate() {
        return create;
    }

    public void setCreate(boolean create) {
        this.create = create;
    }

    public String getBootPassword() {
        return bootPassword;
    }

    public void setBootPassword(String bootPassword) {
        this.bootPassword = bootPassword;
    }

    @Override
    public String getDriverName() {
        return JDBC_DRIVER_APACHE_DERBY;
    }

    @Override
    public String getDbType() {
        return DATABASE_TYPE_APACHE_DERBY;
    }

    @Override
    public Connection createConnection() throws SQLException {
        if (databaseName == null) {
            throw new UnsupportedOperationException("Configuration is incomplete");
        }
        var conStr = String.format("jdbc:derby:%s;create=%b", databaseName, create);
        var props = new Properties();
        if (getUsername() != null) {
            props.putAll(Map.of("user", getUsername(), "password", getPassword()));
        }
        if (bootPassword != null) {
            props.put("bootPassword", bootPassword);
        }
        return DriverManager.getConnection(conStr, props);
    }
}
