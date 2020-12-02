/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import com.hagoapp.f2t.database.config.hive.AuthMech;
import com.hagoapp.f2t.database.config.hive.ServiceDiscoveryMode;
import com.hagoapp.f2t.database.config.hive.TransportMode;

import java.util.ArrayList;
import java.util.List;

public class HiveConfig extends DbConfig {
    private ServiceDiscoveryMode serviceDiscoveryMode;
    private TransportMode transportMode;
    private AuthMech authMech;
    private String host = "";
    private int port = 10000;
    private String zookeeperNamespace;
    private List<ZooKeeperQuorum> quorums;

    public int getPort() {
        return port <= 0 ? 10000 : port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public ServiceDiscoveryMode getServiceDiscoveryMode() {
        return serviceDiscoveryMode;
    }

    public void setServiceDiscoveryMode(ServiceDiscoveryMode serviceDiscoveryMode) {
        this.serviceDiscoveryMode = serviceDiscoveryMode;
    }

    public TransportMode getTransportMode() {
        return transportMode;
    }

    public void setTransportMode(TransportMode transportMode) {
        this.transportMode = transportMode;
    }

    public AuthMech getAuthMech() {
        return authMech;
    }

    public void setAuthMech(AuthMech authMech) {
        this.authMech = authMech;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getZookeeperNamespace() {
        return zookeeperNamespace;
    }

    public void setZookeeperNamespace(String zookeeperNamespace) {
        this.zookeeperNamespace = zookeeperNamespace;
    }

    public List<ZooKeeperQuorum> getQuorums() {
        return quorums;
    }

    public void addQuorum(ZooKeeperQuorum quorum) {
        if (quorums == null) {
            quorums = new ArrayList<>();
        }
        quorums.add(quorum);
    }

    public static class ZooKeeperQuorum {
        private String host;
        private int port = 2181;

        public ZooKeeperQuorum() {

        }

        public ZooKeeperQuorum(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public int getPort() {
            return port <= 0 ? 2181 : port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }
    }

}
