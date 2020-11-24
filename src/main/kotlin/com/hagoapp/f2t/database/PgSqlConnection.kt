/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.database.config.PgSqlConfig
import java.io.Closeable
import java.sql.Connection
import java.sql.DriverManager
import java.sql.JDBCType
import java.util.*

/**
 * Database operations implementation for PostgreSQL.
 */
class PgSqlConnection : DbConnection, Closeable {

    private lateinit var connection: Connection

    companion object {
        private const val PGSQL_DRIVER_CLASS_NAME = "org.postgresql.Driver"

        init {
            Class.forName(PGSQL_DRIVER_CLASS_NAME)
        }
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
        try {
            val ret = mutableMapOf<String, MutableList<TableName>>()
            getConnection(conf).use { con ->
                val sql = """
                    select schemaname, tablename, tableowner from pg_tables 
                    where schemaname<>'pg_catalog' and schemaname<>'information_schema'
                    order by schemaname, tablename
                    """
                con.prepareStatement(sql).use { st ->
                    st.executeQuery().use { rs ->
                        while (rs.next()) {
                            val schema = rs.getString("schemaname")
                            val table = rs.getString("tablename")
                            if (!ret.containsKey(schema)) {
                                ret[schema] = mutableListOf()
                            }
                            ret.getValue(schema).add(TableName(table, schema))
                        }
                        return ret
                    }
                }
            }
        } catch (ex: Exception) {
            return mapOf()
        }
    }

    override fun listDatabases(conf: DbConfig): List<String> {
        if (conf !is PgSqlConfig) {
            throw F2TException("Not a configuration for PostgreSQL")
        }
        conf.databaseName = "postgres"
        try {
            val ret = mutableListOf<String>()
            getConnection(conf).use { con ->
                con.prepareStatement("select datname from pg_database where datistemplate = false and datname != 'postgres'")
                    .use { st ->
                        st.executeQuery().use { rs ->
                            while (rs.next()) {
                                ret.add(rs.getString("datname"))
                            }
                            return ret
                        }
                    }
            }
        } catch (ex: Exception) {
            return listOf()
        }
    }

    private fun getPgConfig(conf: DbConfig): PgSqlConfig {
        if (conf !is PgSqlConfig) {
            throw F2TException("Not a configuration for PostgreSQL")
        }
        return conf
    }

    override fun open(conf: DbConfig) {
        connection = getConnection(conf)
    }

    override fun close() {
        try {
            connection.close()
        } catch (e: Throwable) {
            //
        }
    }

    private fun getConnection(conf: DbConfig): Connection {
        val config = getPgConfig(conf)
        if (config.databaseName.isNullOrBlank()) {
            config.databaseName = "postgres"
        }
        if (listOf(config.host, config.username, conf.password).any { it == null }) {
            throw F2TException("Configuration is incomplete")
        }
        val conStr = "jdbc:postgresql://${config.host}:${config.port}/${config.databaseName}"
        val props = Properties()
        props.putAll(mapOf("user" to config.username, "password" to config.password))
        return DriverManager.getConnection(conStr, props)
    }

    override fun clearTable(table: TableName): Pair<Boolean, String?> {
        try {
            connection.prepareStatement("truncate table ${getFullTableName(table)}").use { st ->
                return Pair(st.execute(), null)
            }
        } catch (ex: Exception) {
            return Pair(false, ex.message)
        }
    }

    override fun dropTable(tableName: String): Pair<Boolean, String?> {
        try {
            connection.prepareStatement("drop table if exists $tableName").use { st ->
                return Pair(st.execute(), null)
            }
        } catch (ex: Exception) {
            return Pair(false, ex.message)
        }
    }

    override fun getWrapperCharacter(): Pair<String, String> {
        return Pair("\"", "\"")
    }

    override fun escapeNameString(name: String): String {
        return name.replace("\"", "\"\"")
    }

    override fun createTable(table: TableName, columnDefinition: Map<String, JDBCType>) {
        val tableFullName = getFullTableName(table)
        val wrapper = getWrapperCharacter()
        val defStr = columnDefinition.map { (col, type) ->
            "${wrapper.first}${escapeNameString(col)}${wrapper.second} ${convertJDBCTypeToDBNativeType(type)}"
        }.joinToString(", ")
        val sql = "create table $tableFullName ($defStr)"
        connection.prepareStatement(sql).use { it.execute() }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType): String {
        return when (aType) {
            JDBCType.BOOLEAN -> "boolean"
            JDBCType.TIMESTAMP -> "timestamp with time zone"
            JDBCType.BIGINT -> "bigint"
            JDBCType.INTEGER -> "int"
            JDBCType.DOUBLE, JDBCType.DECIMAL, JDBCType.FLOAT -> "double precision"
            else -> "text"
        }
    }

    override fun getExistingTableDefinition(table: TableName): Map<String, JDBCType> {
        val sql = """select
            a.attname, format_type(a.atttypid, a.atttypmod) as typename
            from pg_attribute as a
            inner join pg_class as c on c.oid = a.attrelid
            inner join pg_namespace as n on n.oid = c.relnamespace
            where a.attnum > 0 and not a.attisdropped and c.relkind= 'r' and c.relname = ? and n.nspname = ?"""
        connection.prepareStatement(sql).use { stmt ->
            val schema = if (table.schema.isBlank()) getDefaultSchema() else table.schema
            stmt.setString(1, table.tableName)
            stmt.setString(2, schema)
            stmt.executeQuery().use { rs ->
                val tblColDef = mutableMapOf<String, JDBCType>()
                while (rs.next()) {
                    tblColDef[rs.getString("attname")] = mapDBTypeToJDBCType(rs.getString("typename"))
                }
                if (tblColDef.isEmpty()) {
                    throw F2TException(
                        "Column definition of table ${getFullTableName(schema, table.tableName)} not found"
                    )
                }
                return tblColDef
            }
        }
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when {
            typeName.compareTo("integer") == 0 -> JDBCType.INTEGER
            typeName.compareTo("bigint") == 0 -> JDBCType.BIGINT
            typeName.compareTo("boolean") == 0 -> JDBCType.BOOLEAN
            typeName.startsWith("timestamp") -> JDBCType.TIMESTAMP
            typeName.compareTo("double precision") == 0 || typeName.compareTo("real") == 0 ||
                    typeName.startsWith("numeric") -> JDBCType.DOUBLE
            else -> JDBCType.CLOB
        }
    }

    override fun isCaseSensitive(): Boolean {
        return true
    }

    override fun isTableExists(table: TableName): Boolean {
        connection.prepareStatement(
            """select schemaname, tablename, tableowner 
            from pg_tables 
            where tablename = ? and schemaname = ? """
        ).use { stmt ->
            val schema = if (table.schema.isBlank()) getDefaultSchema() else table.schema
            stmt.setString(1, table.tableName)
            stmt.setString(2, schema)
            stmt.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    override fun getDefaultSchema(): String {
        return "public"
    }
}
