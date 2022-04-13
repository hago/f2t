/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

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
}
