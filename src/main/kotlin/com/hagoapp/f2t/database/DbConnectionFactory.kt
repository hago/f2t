/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.F2TLogger
import com.hagoapp.f2t.database.config.DbConfig
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner

class DbConnectionFactory {
    companion object {

        private val typedConnectionMapper = mutableMapOf<DbType, Class<out DbConnection>>()
        private val logger = F2TLogger.getLogger()

        init {
            val r = Reflections(F2TException::class.java.packageName, SubTypesScanner())
            r.getSubTypesOf(DbConnection::class.java).forEach { t ->
                try {
                    val template = t.getConstructor().newInstance()
                    typedConnectionMapper[template.getSupportedDbType()] = t
                    logger.info("DbConnection ${t.canonicalName} registered")
                } catch (e: Exception) {
                    logger.error("Instantiation of class ${t.canonicalName} failed: ${e.message}, skipped")
                }
            }
        }

        @JvmStatic
        fun createDbConnection(dbConfig: DbConfig): DbConnection {
            return when (val clz = typedConnectionMapper[dbConfig.dbType]) {
                null -> throw F2TException("Unknown database type: ${dbConfig.dbType.name}")
                else -> {
                    val con = clz.getConstructor().newInstance()
                    con.open(dbConfig)
                    con
                }
            }
        }
    }
}
