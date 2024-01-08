/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config;

import com.google.gson.GsonBuilder;
import com.hagoapp.f2t.F2TException;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The database configuration factory to read serialized configuration file then creates configuration objects.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
public class DbConfigReader {

    private static final Map<String, Class<? extends DbConfig>> dbConfigMap = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(DbConfigReader.class);

    static {
        var clazz = new Reflections("com.hagoapp", Scanners.SubTypes).getSubTypesOf(DbConfig.class);
        for (var clz : clazz) {
            try {
                var instance = clz.getConstructor().newInstance();
                tryLoadDriver(instance.getDriverName(), clz.getCanonicalName());
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

    private static void tryLoadDriver(String driverName, String className) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            logger.warn("Loading JDBC Driver for {} failed, skipped: {}", className, e.getMessage());
        }

    }

    private DbConfigReader() {
    }

    /**
     * Read configuration objects from serialized JSON file.
     *
     * @param filename JSON file name
     * @return database configuration object
     * @throws F2TException if anything wrong
     */
    public static DbConfig readConfig(String filename) throws F2TException {
        try (var fis = new FileInputStream(filename)) {
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
        return json2DbConfig(new String(content, StandardCharsets.UTF_8));
    }

    /**
     * Read configuration objects from string containing configuration in JSON.
     *
     * @param json JSON file name
     * @return database configuration object
     * @throws F2TException if anything wrong
     */
    public static DbConfig json2DbConfig(String json) throws F2TException {
        var gson = new GsonBuilder().create();
        var baseConfig = gson.fromJson(json, DbConfig.class);
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
