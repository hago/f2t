/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.*
import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.database.config.MariaDbConfig
import java.sql.Connection
import java.sql.DriverManager
import java.sql.JDBCType
import java.util.*

class MariaDBConnection : DbConnection() {

    private lateinit var config: MariaDbConfig

    companion object {
        private const val MARIADB_DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver"

        init {
            Class.forName(MARIADB_DRIVER_CLASS_NAME)
        }
    }

    override fun getSupportedDbType(): DbType {
        return DbType.MariaDb
    }

    override fun canConnect(conf: DbConfig): Pair<Boolean, String> {
        try {
            getConnection(conf).use {
                return Pair(true, "")
            }
        } catch (ex: Exception) {
            return Pair(false, ex.message ?: ex.toString())
        }
    }

    override fun getAvailableTables(conf: DbConfig): Map<String, List<TableName>> {
        val databases = if (!conf.databaseName.isNullOrBlank()) listOf(conf.databaseName) else listDatabases(conf)
        val ret = mutableMapOf<String, List<TableName>>()
        getConnection(conf).use { con ->
            databases.forEach { database ->
                val tables = mutableListOf<TableName>()
                con.prepareStatement("use ${normalizeName(database)}").use { it.execute() }
                con.prepareStatement("show tables").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        while (rs.next()) {
                            val table = rs.getString(1)
                            tables.add(TableName(table, ""))
                        }
                    }
                }
                tables.sortBy { it.tableName }
                ret[database] = tables
            }
            return ret
        }
    }

    override fun listDatabases(conf: DbConfig): List<String> {
        getConnection(conf).use { con ->
            val databases = mutableListOf<String>()
            con.prepareStatement("show databases;").use { stmt ->
                stmt.executeQuery().use { rs ->
                    while (rs.next()) {
                        databases.add(rs.getString(1))
                    }
                }
            }
            return databases
        }
    }

    override fun open(conf: DbConfig) {
        config = checkConfig(conf)
        super.open(conf)
    }

    override fun getConnection(conf: DbConfig): Connection {
        val mariaDbConfig = checkConfig(conf)
        val dbName = if (mariaDbConfig.databaseName.isNullOrBlank()) "information_schema"
        else mariaDbConfig.databaseName
        if (listOf(mariaDbConfig.host, mariaDbConfig.username, mariaDbConfig.password).any { it == null }) {
            throw F2TException("Configuration is incomplete")
        }
        val conStr = "jdbc:mariadb://${mariaDbConfig.host}:${mariaDbConfig.port}/$dbName"
        val props = Properties()
        props.putAll(mapOf("user" to mariaDbConfig.username, "password" to mariaDbConfig.password))
        return DriverManager.getConnection(conStr, props)
    }

    private fun checkConfig(conf: DbConfig): MariaDbConfig {
        if (conf !is MariaDbConfig) {
            throw F2TException("Not a valid MariaDB config")
        }
        return conf
    }

    override fun clearTable(table: TableName): Pair<Boolean, String?> {
        return try {
            connection.prepareStatement("truncate table ${normalizeName(table.tableName)};").use { stmt ->
                stmt.execute()
                connection.commit()
            }
            Pair(true, null)
        } catch (ex: Exception) {
            Pair(false, ex.message)
        }
    }

    override fun dropTable(tableName: String): Pair<Boolean, String?> {
        return try {
            connection.prepareStatement("drop table `$tableName`;").use { stmt ->
                stmt.execute()
                connection.commit()
            }
            Pair(true, null)
        } catch (ex: Exception) {
            Pair(false, ex.message)
        }
    }

    override fun escapeNameString(name: String): String {
        return name.replace("`", "``")
    }

    override fun getWrapperCharacter(): Pair<String, String> {
        return Pair("`", "`")
    }

    override fun isTableExists(table: TableName): Boolean {
        connection.prepareStatement("show tables like '${table.tableName}'").use { stmt ->
            stmt.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    override fun createTable(table: TableName, tableDefinition: TableDefinition) {
        val content = tableDefinition.columns.joinToString(", ") { col ->
            "${normalizeName(col.name)} ${convertJDBCTypeToDBNativeType(col.inferredType!!)} null"
        }
        val sql = """
            create table ${normalizeName(table.tableName)} ($content) 
            engine = ${config.storeEngine} 
            default charset=utf8mb4
            """
        connection.prepareStatement(sql).use { stmt ->
            stmt.execute()
        }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType): String {
        return when (aType) {
            JDBCType.BOOLEAN -> "boolean"
            JDBCType.TIMESTAMP -> "timestamp"
            JDBCType.BIGINT -> "bigint"
            JDBCType.INTEGER -> "int"
            JDBCType.DOUBLE, JDBCType.DECIMAL, JDBCType.FLOAT -> "double"
            else -> "longtext"
        }
    }

    override fun getExistingTableDefinition(table: TableName): TableDefinition {
        val sql = "desc ${normalizeName(table.tableName)};"
        //logger.debug(sql)
        connection.prepareStatement(sql).use { stmt ->
            stmt.executeQuery().use { rs ->
                val def = mutableMapOf<String, JDBCType>()
                while (rs.next()) {
                    def[rs.getString("Field")] = mapDBTypeToJDBCType(rs.getString("Type"))
                }
                return TableDefinition(def.entries.mapIndexed { i, entry ->
                    ColumnDefinition(i, entry.key, mutableSetOf(entry.value), entry.value)
                }.toSet())
            }
        }
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when (typeName.substringBefore('(').toLowerCase()) {
            "tinyint", "bit" -> JDBCType.BOOLEAN
            "int", "smallint", "mediumint" -> JDBCType.INTEGER
            "bigint" -> JDBCType.BIGINT
            "float", "double" -> JDBCType.DOUBLE
            "timestamp", "datetime", "decimal" -> JDBCType.TIMESTAMP
            else -> JDBCType.CLOB
        }
    }

    private var caseSensitive: Boolean? = null
    override fun isCaseSensitive(): Boolean {
        if (this.caseSensitive == null) {
            connection.prepareStatement("show variables like 'lower_case_table_names';").use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    caseSensitive = try {
                        rs.getString("Value").compareTo("0") == 0
                    } catch (ex: Throwable) {
                        true
                    }
                }
            }
        }
        return caseSensitive!!
    }

    override fun getDefaultSchema(): String {
        return ""
    }

}
