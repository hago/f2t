/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.TableUniqueDefinition
import com.hagoapp.f2t.database.config.SqliteConfig
import java.sql.JDBCType
import java.sql.JDBCType.*
import java.sql.SQLException
import kotlin.jvm.Throws

/**
 * Database operations implementation for SQLite 3.
 *
 * @author Chaojun Sun
 * @since 0.8.6
 */
class SqliteConnection : DbConnection() {

    companion object {
        private val SCHEMAS = listOf("main", "temp")
        private const val WRAPPER_PREFIX = "\""
        private const val WRAPPER_SUFFIX = "\""

        data class PragmaTableListResult(
            val schema: String,
            val name: String,
            val type: String,
            val columnNum: Int,
            val withRowId: Boolean,
            val strict: Boolean
        )

        data class PragmaTableInfoResult(
            val name: String,
            val type: String,
            val notNull: Boolean,
            val defaultValue: String?,
            val pk: Int
        )

        data class PragmaUniqueListResult(
            val seq: Int,
            val name: String,
            val unique: Boolean,
            val type: String,
            val partial: Boolean
        )

        data class PragmaUniqueInfoResult(
            val seq: Int,
            val tableSeq: Int,
            val col: String
        )
    }

    override fun getDriverName(): String {
        return SqliteConfig.SQLITE_DRIVER_NAME;
    }

    override fun getAvailableTables(): Map<String, List<TableName>> {
        connection.prepareStatement("PRAGMA table_list").use { st ->
            val tl = mutableListOf<PragmaTableListResult>()
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    tl.add(
                        PragmaTableListResult(
                            rs.getString("schema"),
                            rs.getString("name"),
                            rs.getString("type"),
                            rs.getInt("ncol"),
                            rs.getBoolean("wr"),
                            rs.getBoolean("strict")
                        )
                    )
                }
            }
            return tl.map { TableName(it.name, it.schema) }
                .groupBy { it.schema }
        }
    }

    override fun listDatabases(): List<String> {
        return listOf(connection.catalog)
    }

    override fun escapeNameString(name: String): String {
        return name.replace("\"", "\"\"")
    }

    override fun getWrapperCharacter(): Pair<String, String> {
        return Pair(WRAPPER_PREFIX, WRAPPER_SUFFIX)
    }

    override fun isTableExists(table: TableName): Boolean {
        val tl = getAvailableTables()
        return tl.containsKey(table.schema) && tl.getValue(table.schema).any { it.tableName == table.tableName }
    }

    override fun createTable(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>) {
        checkSchema(table.schema)
        TODO("Not yet implemented")
    }

    @Throws(SQLException::class)
    private fun checkSchema(schema: String) {
        if (!SCHEMAS.contains(schema.lowercase())) {
            throw SQLException("Invalid schema '$schema'")
        }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String {
        TODO("Not yet implemented")
    }

    override fun getExistingTableDefinition(table: TableName): TableDefinition<in ColumnDefinition> {
        val sql = "PRAGMA ${table.schema}.table_info(${normalizeName(table.tableName)})"
        val cols: List<ColumnDefinition>
        connection.prepareStatement(sql).use { st ->
            val tl = mutableListOf<PragmaTableInfoResult>()
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    tl.add(
                        PragmaTableInfoResult(
                            rs.getString("name"),
                            rs.getString("type"),
                            rs.getBoolean("notnull"),
                            rs.getString("dflt_value"),
                            rs.getInt("pk")
                        )
                    )
                }
            }
            cols = tl.map {
                val col = ColumnDefinition(it.name)
                val typeParts = parseTypeName(it.type)
                col.databaseTypeName = typeParts.first
                col.dataType = mapDBTypeToJDBCType(typeParts.first)
                col.typeModifier = typeParts.second
                col.typeModifier.isContainsNonAscii = col.dataType == CLOB
                col.typeModifier.isNullable = !it.notNull
                col
            }
        }
        val pk = getPrimaryKey(table, cols)
        val def = TableDefinition(cols, isCaseSensitive(), pk, false)
        def.uniqueConstraints = getUniqueConstraints(table, cols)
        return def
    }

    private fun parseTypeName(type: String): Pair<String, ColumnTypeModifier> {
        val s = type.indexOf('(')
        return if (s < 0) {
            Pair(type, ColumnTypeModifier(0, 0, 0, null, true, false))
        } else {
            val ms = type.substring(s + 1, type.length - 1)
            val acc = ms.split(",").map { it.toInt() }
            val modifier = ColumnTypeModifier(
                if (acc.size > 1) 0 else ms.toInt(),
                if (acc.size > 1) acc[0] else 0,
                if (acc.size > 1) acc[1] else 0, null, true, false
            )
            Pair(type.substring(0, s), modifier)
        }
    }

    private fun getPrimaryKey(
        table: TableName,
        columns: List<ColumnDefinition>
    ): TableUniqueDefinition<ColumnDefinition>? {
        return getIndexes(table, "pk", columns).firstOrNull()
    }

    private fun getUniqueConstraints(table: TableName, columns: List<ColumnDefinition>):
            Set<TableUniqueDefinition<ColumnDefinition>> {
        return getIndexes(table, "u", columns).toSet()
    }

    private fun getIndexes(table: TableName, indexType: String, columns: List<ColumnDefinition>):
            List<TableUniqueDefinition<ColumnDefinition>> {
        val sql = "PRAGMA ${table.schema}.index_list(${normalizeName(table.tableName)})"
        val indices = mutableListOf<PragmaUniqueListResult>()
        connection.prepareStatement(sql).use { st ->
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    val t = rs.getString(4)
                    if (t == indexType) {
                        indices.add(
                            PragmaUniqueListResult(
                                rs.getInt(1),
                                rs.getString(2),
                                rs.getBoolean(3),
                                t,
                                rs.getBoolean(5)
                            )
                        )
                    }
                }
            }
        }
        return indices.map { index ->
            val sql0 = "PRAGMA ${table.schema}.index_info(${normalizeName(index.name)})"
            val indexColumnNames = mutableListOf<String>()
            connection.prepareStatement(sql0).use { st ->
                st.executeQuery().use { rs ->
                    while (rs.next()) {
                        indexColumnNames.add(rs.getString(3))
                    }
                }
            }
            TableUniqueDefinition(
                index.name,
                columns.filter { column -> indexColumnNames.contains(column.name) },
                isCaseSensitive()
            )
        }
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        val t = typeName.lowercase()
        return when {
            t.contains("int") -> INTEGER
            listOf("char", "text", "clob").any { t.contains(it) } -> CLOB
            t.contains("blob") -> BLOB
            listOf("real", "floa", "doub").any { t.contains(it) } -> DOUBLE
            else -> DECIMAL
        }
    }

    override fun isCaseSensitive(): Boolean {
        return false
    }

    override fun getDefaultSchema(): String {
        return SCHEMAS[0]
    }
}
