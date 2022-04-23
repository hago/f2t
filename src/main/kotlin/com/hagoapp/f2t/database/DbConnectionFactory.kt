/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.F2TLogger
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import java.sql.Connection

/**
 * This is a factory class to create database target implementation. It will search any descendants of
 * <code>DbConnection</code> in default and additional packages which could be registered by calling
 * <code>registerPackageNames</code> method.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
class DbConnectionFactory {
    companion object {

        private val typedConnectionMapper = mutableMapOf<String, Class<out DbConnection>>()
        private val logger = F2TLogger.getLogger()

        init {
            registerPackageNames(F2TLogger::class.java.packageName)
        }

        /**
         * Register additional packages to search customized database implementations.
         *
         * @param packageNames  package names
         */
        @JvmStatic
        fun registerPackageNames(vararg packageNames: String) {
            for (packageName in packageNames) {
                val r = Reflections(packageName, Scanners.SubTypes)
                r.getSubTypesOf(DbConnection::class.java).forEach { t ->
                    try {
                        val template = t.getConstructor().newInstance()
                        typedConnectionMapper[template.getDriverName().lowercase()] = t
                        logger.info("DbConnection ${t.canonicalName} registered")
                    } catch (e: Exception) {
                        logger.error("Instantiation of class ${t.canonicalName} failed: ${e.message}, skipped")
                    }
                }
            }
        }

        /**
         * Create database connection object on given <code>DbConfig</code> sub-type.
         *
         * @param connection    SQL connection, subtype of <code>Java.Sql.Connection</code>
         * @return Database implementation instance
         */
        @JvmStatic
        fun createDbConnection(connection: Connection): DbConnection {
            val database = connection.metaData.databaseProductName.lowercase()
            return when (val clz = typedConnectionMapper[database]) {
                null -> throw F2TException("Unknown database type: $database")
                else -> {
                    val con = clz.getConstructor().newInstance()
                    con.open(connection)
                    con
                }
            }
        }
    }
}
