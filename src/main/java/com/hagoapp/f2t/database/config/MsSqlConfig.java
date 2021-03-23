/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import com.hagoapp.f2t.database.DbType;

public class MsSqlConfig extends DbConfig {

    public MsSqlConfig() {
        super();
        dbType = DbType.MsSqlServer;
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
