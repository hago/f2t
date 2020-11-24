/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.datafile.ColumnDefinition
import java.sql.JDBCType

/**
 * Interface of database operations required to insert a set of 2-dimensional data into it.
 */
interface DbConnection {
    companion object {
        /**
         * Acquire a DbConnection object according the type.
         */
        fun getDbConnection(type: DbType): DbConnection {
            return when (type) {
                DbType.PostgreSql -> PgSqlConnection()
                DbType.MsSqlServer,
                DbType.MariaDb,
                DbType.Hive -> TODO()
            }
        }

        fun getDbConnection(conf: DbConfig): DbConnection {
            return getDbConnection(conf.dbType)
        }
    }

    /**
     * Whether the config is valid to lead a successful connection.
     */
    fun canConnect(conf: DbConfig): Pair<Boolean, String>

    /**
     * Fetch the existing tables from database.
     */
    fun getAvailableTables(conf: DbConfig): Map<String, List<TableName>>

    /**
     * List all visible databases by user from the config.
     */
    fun listDatabases(conf: DbConfig): List<String>

    /**
     * Create the JDBC connection when needed. This method could be called many other methods in implementation.
     */
    fun open(conf: DbConfig)

    /**
     * Clear any data in given table.
     */
    fun clearTable(table: TableName): Pair<Boolean, String?>

    /**
     * Drop given table.
     */
    fun dropTable(tableName: String): Pair<Boolean, String?>
    fun dropTable(table: TableName): Pair<Boolean, String?> {
        return dropTable(getFullTableName(table))
    }

    /**
     * Create quoted, escaped identity name of database.
     */
    fun normalizeName(name: String): String {
        return if (isNormalizedName(name)) name else {
            val wrapper = getWrapperCharacter()
            "${wrapper.first}${escapeNameString(name)}${wrapper.second}"
        }
    }

    /**
     * Whether the given name is quoted, escaped on database's definition.
     */
    fun isNormalizedName(name: String): Boolean {
        val w = getWrapperCharacter()
        return name.trim().startsWith(w.first) && name.trim().endsWith(w.second)
    }

    /**
     * Escape database identity name.
     */
    fun escapeNameString(name: String): String

    /**
     * Get the wrapper character defined by database.
     */
    fun getWrapperCharacter(): Pair<String, String>

    /**
     * Create quoted, escaped table name of database, including schema if supported.
     */
    fun getFullTableName(schema: String, tableName: String): String {
        return if (schema.isBlank()) normalizeName(tableName)
        else "${normalizeName(schema)}.${normalizeName(tableName)}"
    }

    fun getFullTableName(table: TableName): String {
        return getFullTableName(table.schema, table.tableName)
    }

    /**
     * Whether the given table exists in database by schema and name.
     */
    fun isTableExists(table: TableName): Boolean

    /**
     * Create a new table on given name and column definitions.
     */
    fun createTable(table: TableName, columnDefinition: List<ColumnDefinition>)

    /**
     * Find local database type on given JDBC type.
     */
    fun convertJDBCTypeToDBNativeType(aType: JDBCType): String

    /**
     * Fetch column definitions of given table.
     */
    fun getExistingTableDefinition(table: TableName): List<ColumnDefinition>

    /**
     * Find JDBC type on database local type.
     */
    fun mapDBTypeToJDBCType(typeName: String): JDBCType

    /**
     * Whether this database is case sensitive.
     */
    fun isCaseSensitive(): Boolean

    /**
     * Get default schema of database.
     */
    fun getDefaultSchema(): String

    /**
     * Set the record amount of one batch insert.
     */
    fun getInsertBatchAmount(): Long {
        return 1000
    }

    /**
     * this method is for those database / JDBC drivers that doesn't support certain kind
     * of data types, e.g. timestamp in hive.
     * It should convert a value with specific unsupported type to new value in supported
     * type, to allow database inserting functions to create a compatible JDBC statement.
     */
    fun getTypedDataConverters(): Map<JDBCType, (Any) -> Pair<JDBCType, Any?>> {
        return mapOf()
    }
}
