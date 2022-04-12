/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.F2TLogger;
import com.hagoapp.f2t.database.DbConnection;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * The database configuration factory to read serialized configuration file then creates configuration objects.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
public class DbConfigReader {

    private final static Map<String, Class<? extends DbConfig>> dbConfigMap = new HashMap<>();
    private final static Logger logger = F2TLogger.getLogger();

    static {
        var clazz = new Reflections("com.hagoapp", Scanners.SubTypes).getSubTypesOf(DbConfig.class);
        for (var clz : clazz) {
            try {
                var instance = clz.getConstructor().newInstance();
                var i = instance.getDbType();
                if (dbConfigMap.containsKey(i)) {
                    logger.warn("duplicate database type {} for {}  and {}, {} is ignored", i,
                            dbConfigMap.get(i).getCanonicalName(), clz.getCanonicalName(), clz.getCanonicalName());
                } else {
                    logger.info("db type {} for {}", i, clz.getCanonicalName());
                    dbConfigMap.put(i.toLowerCase(), clz);
                }
            } catch (InstantiationException | IllegalAccessException |
                    InvocationTargetException | NoSuchMethodException e) {
                logger.error("error {} occurs in instantiating {}", e.getMessage(), clz.getCanonicalName());
            }
        }
    }

    /**
     * Read configuration objects from serialized JSON file.
     *
     * @param filename JSON file name
     * @return database configuration object
     * @throws F2TException if anything wrong
     */
    public static DbConfig readConfig(String filename) throws F2TException {
        try (FileInputStream fis = new FileInputStream(filename)) {
            return readConfig(fis);
        } catch (IOException e) {
            throw new F2TException(String.format("Load FileInfo object from file %s failed", filename), e);
        }
    }

    /**
     * Read configuration objects from a stream containing configuration in JSON.
     *
     * @param stream JSON stream
     * @return database configuration object
     * @throws F2TException if anything wrong
     */
    public static DbConfig readConfig(InputStream stream) throws F2TException {
        try {
            return readConfig(stream.readAllBytes());
        } catch (IOException e) {
            throw new F2TException("Read DB config error: " + e.getMessage(), e);
        }
    }

    /**
     * Read configuration objects from byte sequences containing configuration in JSON.
     *
     * @param content JSON bytes
     * @return database configuration object
     * @throws F2TException if anything wrong
     */
    public static DbConfig readConfig(byte[] content) throws F2TException {
        return json2DbConfig(new String(content));
    }

    /**
     * Read configuration objects from string containing configuration in JSON.
     *
     * @param json JSON file name
     * @return database configuration object
     * @throws F2TException if anything wrong
     */
    public static DbConfig json2DbConfig(String json) throws F2TException {
        Gson gson = new GsonBuilder().create();
        DbConfig baseConfig = gson.fromJson(json, DbConfig.class);
        if (baseConfig == null) {
            throw new F2TException("Not a valid db config, check whether 'dbType' existed and valid.");
        }
        var i = baseConfig.getDbType().toLowerCase();
        var clz = dbConfigMap.get(i);
        if (clz == null) {
            throw new F2TException(String.format("DB config type %s not supported", i));
        } else {
            return gson.fromJson(json, clz);
        }
    }
}
