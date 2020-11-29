/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.database.config;

public class MariaDbConfig extends DbConfig {
    private String host;
    private Integer port;
    private String socketFile;
    private String storeEngine;

    public String getStoreEngine() {
        return storeEngine;
    }

    public void setStoreEngine(String storeEngine) {
        this.storeEngine = storeEngine;
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

    public String getSocketFile() {
        return socketFile;
    }

    public void setSocketFile(String socketFile) {
        this.socketFile = socketFile;
    }
}
