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
import com.hagoapp.f2t.util.ColumnMatcher
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

    override fun getSupportedDbType(): String {
        return MariaDbConfig.DATABASE_TYPE_MARIADB
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

    override fun createTable(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>) {
        val content = tableDefinition.columns.joinToString(", ") { col ->
            "${normalizeName(col.name)} ${convertJDBCTypeToDBNativeType(col.dataType!!, col.typeModifier)} null"
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

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String {
        return when (aType) {
            JDBCType.BOOLEAN -> "boolean"
            JDBCType.TIMESTAMP -> "timestamp"
            JDBCType.TINYINT -> "tinyint"
            JDBCType.SMALLINT -> "smallint"
            JDBCType.INTEGER -> "int"
            JDBCType.BIGINT -> "bigint"
            JDBCType.DOUBLE -> "double"
            JDBCType.DECIMAL -> "decimal"
            JDBCType.FLOAT -> "float"
            else -> "longtext"
        }
    }

    private data class DescResult(
        val field: String,
        val typeName: String,
        val type: JDBCType,
        val isNullable: Boolean
    )

    override fun getExistingTableDefinition(table: TableName): TableDefinition<ColumnDefinition> {
        val sql = "desc ${normalizeName(table.tableName)};"
        //logger.debug(sql)
        connection.prepareStatement(sql).use { stmt ->
            stmt.executeQuery().use { rs ->
                val def = mutableListOf<DescResult>()
                while (rs.next()) {
                    val dr = DescResult(
                        rs.getString("Field"),
                        rs.getString("Type"),
                        mapDBTypeToJDBCType(rs.getString("Type")),
                        rs.getBoolean("Null")
                    )
                    def.add(dr)
                }
                val td = TableDefinition(def.map { desc ->
                    val cd = ColumnDefinition(desc.field, desc.type)
                    setupColumnDefinition(cd, desc.typeName, desc.isNullable)
                    cd
                }.toSet())
                val uniques = getIndexes(table, td.columns)
                td.primaryKey = uniques.first
                td.uniqueConstraints = uniques.second
                return td
            }
        }
    }

    private fun setupColumnDefinition(columnDefinition: ColumnDefinition, typeName: String, nullable: Boolean) {
        val tm = columnDefinition.typeModifier
        tm.isNullable = nullable
        val extra = parseModifier(typeName)
        tm.maxLength = extra.first
        tm.precision = extra.second
        tm.scale = extra.third
    }

    private fun parseModifier(typeStr: String): Triple<Int, Int, Int> {
        return when {
            typeStr.startsWith("char") || typeStr.startsWith("varchar") -> {
                val m = Regex(".+?\\((\\d+)\\)").matchEntire(typeStr)
                if ((m != null) && m.groupValues.isNotEmpty()) Triple(m.groupValues.last().toInt(), 0, 0)
                else Triple(0, 0, 0)
            }
            typeStr.startsWith("decimal") -> {
                val m = Regex(".+?\\((\\d+),(\\d+)\\)").matchEntire(typeStr)
                if ((m != null) && (m.groupValues.size > 2))
                    Triple(0, m.groupValues[1].toInt(), m.groupValues[2].toInt())
                else Triple(0, 0, 0)
            }
            else -> Triple(0, 0, 0)
        }
    }

    private fun getIndexes(
        table: TableName,
        refColumns: Set<ColumnDefinition>
    ): Pair<TableUniqueDefinition<ColumnDefinition>?, Set<TableUniqueDefinition<ColumnDefinition>>> {
        val sql = "show indexes from ${normalizeName(table.tableName)}"
        connection.prepareStatement(sql).use { stmt ->
            val keys = mutableMapOf<String, MutableSet<String>>()
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    if (rs.getInt("Non_unique") != 0) {
                        continue
                    }
                    val keyName = rs.getString("Key_name")
                    val colName = rs.getString("Column_name")
                    if (keys.containsKey(keyName)) {
                        keys.getValue(keyName).add(colName)
                    } else {
                        keys[keyName] = mutableSetOf(colName)
                    }
                }
            }
            return Pair(
                keys.filter { (key, _) -> key == "PRIMARY" }.firstNotNullOfOrNull {
                    buildUniqueDef(it.key, it.value, refColumns)
                },
                keys.filter { (key, _) -> key != "PRIMARY" }.map {
                    buildUniqueDef(it.key, it.value, refColumns)
                }.toSet()
            )
        }
    }

    private fun buildUniqueDef(
        name: String,
        columns: Set<String>,
        refColumns: Set<ColumnDefinition>
    ): TableUniqueDefinition<ColumnDefinition> {
        val colMatcher = ColumnMatcher.getColumnMatcher(isCaseSensitive())
        val tud = TableUniqueDefinition(name, columns.map { col ->
            refColumns.first { colMatcher(col, it.name) }
        }.toSet(), isCaseSensitive())
        return tud
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when (typeName.substringBefore('(').lowercase()) {
            "boolean" -> JDBCType.BOOLEAN
            "tinyint" -> JDBCType.TINYINT
            "smallint" -> JDBCType.SMALLINT
            "int", "mediumint" -> JDBCType.INTEGER
            "bigint" -> JDBCType.BIGINT
            "float" -> JDBCType.FLOAT
            "double" -> JDBCType.DOUBLE
            "decimal", "dec", "numeric", "fixed" -> JDBCType.DECIMAL
            "timestamp", "datetime" -> JDBCType.TIMESTAMP_WITH_TIMEZONE
            "date" -> JDBCType.DATE
            "time" -> JDBCType.TIME
            "char" -> JDBCType.CHAR
            "varchar" -> JDBCType.VARCHAR
            "text", "longtext" -> JDBCType.CLOB
            "binary", "blob", "char byte" -> JDBCType.VARBINARY
            else -> throw java.lang.UnsupportedOperationException("unsupported type $typeName")
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
