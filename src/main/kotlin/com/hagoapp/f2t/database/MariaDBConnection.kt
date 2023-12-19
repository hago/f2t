/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.*
import com.hagoapp.f2t.database.config.MariaDbConfig
import com.hagoapp.f2t.util.ColumnMatcher
import java.sql.JDBCType
import java.sql.JDBCType.*
import java.sql.SQLException

/**
 * Database operation implementation for MariaDB / MySQL.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
open class MariaDBConnection : DbConnection() {

    companion object {
        private const val MARIADB_DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver"
    }

    override fun getDriverName(): String {
        return MARIADB_DRIVER_CLASS_NAME
    }

    override fun getAvailableTables(): Map<String, List<TableName>> {
        val databases = listDatabases()
        val ret = mutableMapOf<String, List<TableName>>()
        val defaultDb = connection.prepareStatement("select database()").use { stmt ->
            stmt.executeQuery().use { rs ->
                rs.next()
                rs.getString(1)
            }
        }
        databases.forEach { database ->
            val tables = mutableListOf<TableName>()
            connection.prepareStatement("use ${normalizeName(database)}").use { it.execute() }
            connection.prepareStatement("show tables").use { stmt ->
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
        connection.prepareStatement("use $defaultDb").use { it.execute() }
        return ret
    }

    override fun listDatabases(): List<String> {
        val databases = mutableListOf<String>()
        connection.prepareStatement("show databases;").use { stmt ->
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    databases.add(rs.getString(1))
                }
            }
        }
        return databases
    }

    override fun clearTable(table: TableName): Pair<Boolean, String?> {
        return try {
            connection.prepareStatement("truncate table ${getFullTableName(table)};").use { stmt ->
                stmt.execute()
                connection.commit()
            }
            Pair(true, null)
        } catch (ex: SQLException) {
            Pair(false, ex.message)
        }
    }

    override fun dropTable(tableName: String): Pair<Boolean, String?> {
        return try {
            connection.prepareStatement("drop table ${normalizeName(tableName)};").use { stmt ->
                stmt.execute()
                connection.commit()
            }
            Pair(true, null)
        } catch (ex: SQLException) {
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
        connection.prepareStatement("show tables like '${table.tableName.replace("'", "''")}'")
            .use { stmt ->
                stmt.executeQuery().use { rs ->
                    return rs.next()
                }
            }
    }

    override fun createTable(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>) {
        var content = tableDefinition.columns.joinToString(", ") { col ->
            val colDef = "${normalizeName(col.name)} ${convertJDBCTypeToDBNativeType(col.dataType!!, col.typeModifier)}"
            val nullable = if (col.typeModifier.isNullable) "null" else "not null"
            "$colDef $nullable"
        }
        val engine = extraProperties[MariaDbConfig.STORE_ENGINE_NAME]?.toString()
            ?: MariaDbConfig.DEFAULT_STORE_ENGINE_INNODB
        val primaryKeyDef = if (tableDefinition.primaryKey?.columns == null) {
            null
        } else {
            val p = tableDefinition.primaryKey!!
            "constraint ${normalizeName(p.name)} primary key (${
                p.columns.joinToString(", ") { normalizeName(it.name) }
            })"
        }
        val uniqueDef = if (tableDefinition.uniqueConstraints.isEmpty()) {
            null
        } else {
            tableDefinition.uniqueConstraints.joinToString(",") {
                val head = "CONSTRAINT ${normalizeName(it.name)} unique "
                val uniqueCols = it.columns.joinToString(",") { col -> normalizeName(col.name) }
                head + uniqueCols
            }
        }
        content = if (primaryKeyDef == null) content else "$content, $primaryKeyDef"
        if (uniqueDef != null) {
            content += ", $uniqueDef"
        }
        val sql = """
            create table ${getFullTableName(table)} ($content) 
            engine = $engine
            default charset=utf8mb4
            """
        connection.prepareStatement(sql).use { stmt ->
            stmt.execute()
        }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String {
        return when (aType) {
            BOOLEAN -> "boolean"
            TINYINT -> "tinyint"
            SMALLINT -> "smallint"
            INTEGER -> "int"
            BIGINT -> "bigint"
            DOUBLE -> "double"
            DECIMAL -> "decimal(${modifier.precision}, ${modifier.scale})"
            FLOAT -> "float"
            CHAR, NCHAR -> "char(${modifier.maxLength})"
            VARCHAR, NVARCHAR -> "varchar(${modifier.maxLength})"
            CLOB, NCLOB -> "longtext"
            DATE -> "date"
            TIME, TIME_WITH_TIMEZONE -> "time"
            TIMESTAMP_WITH_TIMEZONE, TIMESTAMP -> "timestamp"
            BIT -> "bit"
            BLOB -> "blob"
            else -> "binary"
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
                })
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
        columnDefinition.databaseTypeName = typeName
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

    private data class MariaDbUniqueIndexItem(
        val name: String,
        val columnName: String,
        val columnSeq: Int
    )

    private fun getIndexes(
        table: TableName,
        refColumns: List<ColumnDefinition>
    ): Pair<TableUniqueDefinition<ColumnDefinition>?, Set<TableUniqueDefinition<ColumnDefinition>>> {
        val sql = "show indexes from ${normalizeName(table.tableName)}"
        connection.prepareStatement(sql).use { stmt ->
            val keyColumnItemList = mutableListOf<MariaDbUniqueIndexItem>()
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    if (rs.getInt("Non_unique") != 0) {
                        continue
                    }
                    val ki = MariaDbUniqueIndexItem(
                        rs.getString("Key_name"),
                        rs.getString("Column_name"),
                        rs.getInt("Seq_in_index"),
                    )
                    keyColumnItemList.add(ki)
                }
            }
            val keys = keyColumnItemList.groupBy { it.name }
                .mapValues { it.value.sortedBy { item -> item.columnSeq }.map { item -> item.columnName } }
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
        columns: List<String>,
        refColumns: List<ColumnDefinition>
    ): TableUniqueDefinition<ColumnDefinition> {
        val colMatcher = ColumnMatcher.getColumnMatcher(isCaseSensitive())
        val tud = TableUniqueDefinition(name, columns.map { col ->
            refColumns.first { colMatcher(col, it.name) }
        }, isCaseSensitive())
        return tud
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        val type = typeName.substringBefore('(').lowercase()
        return when {
            type == "bit" -> BIT
            type == "boolean" || type == "bool" -> BOOLEAN
            type == "timestamp" || type == "datetime" -> TIMESTAMP_WITH_TIMEZONE
            type == "text" || type == "longtext" || type == "mediumtext" || type == "tinytext" -> CLOB
            type.startsWith("tinyint") -> TINYINT
            type.startsWith("smallint") -> SMALLINT
            type.startsWith("int") || type.startsWith("mediumint") || type.startsWith("integer") -> INTEGER
            type.startsWith("bigint") -> BIGINT
            type.startsWith("float") || type.startsWith("real") -> FLOAT
            type.startsWith("double") -> DOUBLE
            type in listOf("decimal", "dec", "numeric", "fixed") -> DECIMAL
            type in listOf("date", "year") -> DATE
            type in listOf("time") -> TIME
            type in listOf("char", "enum", "set") -> CHAR
            type in listOf("varchar", "long varchar") -> VARCHAR
            type in listOf("binary", "char byte", "long varbinary", "varbinary") -> VARBINARY
            type in listOf("blob", "longblob", "mediumblob", "tinyblob") -> BLOB
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
                    } catch (ex: SQLException) {
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
