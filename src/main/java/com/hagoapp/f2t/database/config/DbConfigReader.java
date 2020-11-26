/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hagoapp.f2t.F2TException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DbConfigReader {
    public static DbConfig readConfig(String filename) throws F2TException {
        try (FileInputStream fis = new FileInputStream(filename)) {
            return readConfig(fis);
        } catch (IOException e) {
            throw new F2TException(String.format("Load FileInfo object from file %s failed", filename), e);
        }
    }

    public static DbConfig readConfig(InputStream stream) throws F2TException {
        try {
            return readConfig(stream.readAllBytes());
        } catch (IOException e) {
            throw new F2TException("Read DB config error: " + e.getMessage(), e);
        }
    }

    public static DbConfig readConfig(byte[] content) throws F2TException {
        return json2DbConfig(new String(content));
    }

    public static DbConfig json2DbConfig(String json) throws F2TException {
        Gson gson = new GsonBuilder().create();
        DbConfig baseConfig = gson.fromJson(json, DbConfig.class);
        switch (baseConfig.getDbType()) {
            case PostgreSql:
                return gson.fromJson(json, PgSqlConfig.class);
            default:
                throw new F2TException("DB config type unknown");
        }
    }
}
