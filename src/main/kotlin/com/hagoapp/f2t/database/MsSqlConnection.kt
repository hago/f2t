/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.database.config.MsSqlConfig
import java.sql.Connection
import java.sql.DriverManager
import java.sql.JDBCType
import java.util.*

class MsSqlConnection : DbConnection() {

    private lateinit var msConfig: MsSqlConfig

    companion object {
        private const val MSSQL_DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        init {
            Class.forName(MSSQL_DRIVER_CLASS_NAME)
        }
    }

    override fun getSupportedDbType(): DbType {
        return DbType.MsSqlServer
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
            val sql = """
                select s.name as [schema], o.name as [table] 
                from sys.schemas as s inner join sys.objects as o on s.schema_id = o.schema_id 
                where o.[type]='U'
                order by s.name, o.name
                """
            getConnection(conf).use { con ->
                con.prepareStatement(sql).use { st ->
                    st.executeQuery().use { rs ->
                        while (rs.next()) {
                            val schema = rs.getString("schema")
                            val table = rs.getString("table")
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
            logger.error("fetch table list error: $ex")
            // println(ex)
            return mapOf()
        }
    }

    override fun listDatabases(conf: DbConfig): List<String> {
        try {
            val ret = mutableListOf<String>()
            getConnection(conf).use { con ->
                val sql = "select name from sys.databases where name not in ('master', 'tempdb', 'msdb', 'model')"
                con.prepareStatement(sql).use { st ->
                    st.executeQuery().use { rs ->
                        while (rs.next()) {
                            ret.add(rs.getString("name"))
                        }
                        return ret
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error("fetch database list error: $ex")
            // println(ex)
            return listOf()
        }
    }

    override fun open(conf: DbConfig) {
        msConfig = getConfig(conf)
        super.open(conf)
    }

    override fun getConnection(conf: DbConfig): Connection {
        val config = getConfig(conf)
        if (config.databaseName.isNullOrBlank()) {
            config.databaseName = "master"
        }
        if (listOf(config.host, config.username, config.password).any { it == null }) {
            throw F2TException("Configuration is incomplete")
        }
        val conStr = "jdbc:sqlserver://${config.host}:${config.port};databaseName = ${config.databaseName}"
        val props = Properties()
        props.putAll(mapOf("user" to config.username, "password" to config.password))
        return DriverManager.getConnection(conStr, props)
    }

    private fun getConfig(conf: DbConfig): MsSqlConfig {
        if (conf !is MsSqlConfig) {
            throw F2TException("Not a MS SQL config")
        }
        return conf
    }

    override fun clearTable(table: TableName): Pair<Boolean, String?> {
        try {
            connection.prepareStatement("truncate table ${getFullTableName(table)}").use { st ->
                return Pair(st.execute(), null)
            }
        } catch (ex: Throwable) {
            return Pair(false, ex.message)
        }
    }

    override fun dropTable(tableName: String): Pair<Boolean, String?> {
        try {
            connection.prepareStatement("drop table ${normalizeName(tableName)};").use { st ->
                return Pair(st.execute(), null)
            }
        } catch (ex: Exception) {
            return Pair(false, ex.message)
        }
    }

    override fun escapeNameString(name: String): String {
        return name.replace("]", "]]")
    }

    override fun getWrapperCharacter(): Pair<String, String> {
        return Pair("[", "]")
    }

    override fun isTableExists(table: TableName): Boolean {
        val sql = """SELECT distinct T2.name AS schemaname,t1.name AS tablename FROM sys.sysobjects T1
                inner join sys.schemas T2 ON (T2.[schema_id] = T1.[uid])
                WHERE xtype = 'U' and t2.name = ? and t1.name = ?"""
        connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, if (table.schema.isBlank()) getDefaultSchema() else table.schema)
            stmt.setString(2, table.tableName)
            stmt.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    override fun createTable(table: TableName, tableDefinition: TableDefinition) {
        val content = tableDefinition.columns.joinToString(", ") { col ->
            "${normalizeName(col.name)} ${convertJDBCTypeToDBNativeType(col.inferredType!!)}"
        }
        val sql = "create table ${getFullTableName(table)} ($content);"
        logger.debug("create table using SQL: $sql")
        connection.prepareStatement(sql).use { stmt ->
            stmt.execute()
        }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType): String {
        return when (aType) {
            JDBCType.BOOLEAN -> "bit"
            JDBCType.TIMESTAMP -> "datetimeoffset"
            JDBCType.BIGINT -> "bigint"
            JDBCType.INTEGER -> "int"
            JDBCType.DOUBLE, JDBCType.DECIMAL, JDBCType.FLOAT -> "float"
            else -> "ntext"
        }
    }

    override fun getExistingTableDefinition(table: TableName): TableDefinition {
        val sql = """
                select TYPE_NAME(c.system_type_id) as typeName, c.name
                from  sys.schemas as s
                inner join sys.objects as o on s.schema_id = o.schema_id
                inner join sys.columns as c on o.object_id = c.object_id
                where o.name=? and o.type='U' and s.name = ?
            """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, table.tableName)
            stmt.setString(2, if (table.schema.isBlank()) getDefaultSchema() else table.schema)
            stmt.executeQuery().use { rs ->
                val tblColDef = mutableMapOf<String, JDBCType>()
                while (rs.next()) {
                    tblColDef[rs.getString("name")] = mapDBTypeToJDBCType(rs.getString("typeName"))
                }

                return TableDefinition(tblColDef.entries.mapIndexed { i, col ->
                    ColumnDefinition(i, col.key, mutableSetOf(col.value), col.value)
                }.toSet())
            }
        }
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when {
            typeName.compareTo("integer") == 0 -> JDBCType.INTEGER
            typeName.compareTo("bigint") == 0 -> JDBCType.BIGINT
            typeName.compareTo("bit") == 0 -> JDBCType.BOOLEAN
            typeName.startsWith("datetimeoffset") -> JDBCType.TIMESTAMP
            typeName.compareTo("float") == 0 || typeName.compareTo("real") == 0 ||
                    typeName.startsWith("numeric") -> JDBCType.DOUBLE
            else -> JDBCType.CLOB
        }
    }

    private var caseSensitive: Boolean? = null
    override fun isCaseSensitive(): Boolean {
        if (caseSensitive == null) {
            val sql =
                "select convert(varchar, DATABASEPROPERTYEX('${msConfig.databaseName}', 'collation')) as SQLCollation"
            caseSensitive = connection.prepareStatement(sql).use { stmt ->
                stmt.executeQuery().use { rs ->
                    rs.next()
                    !rs.getString("SQLCollation").split('_').contains("CI")
                }
            }
        }
        return caseSensitive!!
    }

    override fun getDefaultSchema(): String {
        return "dbo"
    }
}
