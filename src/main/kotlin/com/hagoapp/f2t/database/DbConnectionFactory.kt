/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.database.config.DbConfig

class DbConnectionFactory {
    companion object {
        @JvmStatic
        fun createDbConnection(dbConfig: DbConfig): DbConnection {
            val connection = when(dbConfig.dbType) {
                DbType.PostgreSql -> PgSqlConnection()
                DbType.MariaDb -> MariaDBConnection()
                DbType.Hive -> HiveConnection()
                DbType.MsSqlServer -> MsSqlConnection()
                else -> throw F2TException("Unknown database type: ${dbConfig.dbType.name}")
            }
            connection.open(dbConfig)
            return connection
        }
    }
}
