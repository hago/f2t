/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.F2TLogger
import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.TableDefinition
import org.slf4j.Logger
import java.io.Closeable
import java.sql.*
import java.time.ZonedDateTime

/**
 * Interface of database operations required to insert a set of 2-dimensional data into it.
 */
abstract class DbConnection : Closeable {

    protected lateinit var connection: Connection
    protected val insertionMap = mutableMapOf<TableName, String>()
    protected val rows = mutableListOf<DataRow>()
    protected val logger: Logger = F2TLogger.getLogger()
    protected val fieldValueSetters =
        mutableMapOf<TableName, List<(stmt: PreparedStatement, i: Int, value: Any?) -> Unit>>()

    /**
     * Tell user what kind of db config is required
     */
    abstract fun getSupportedDbType(): String

    /**
     * Whether the config is valid to lead a successful connection.
     */
    abstract fun canConnect(conf: DbConfig): Pair<Boolean, String>

    /**
     * Fetch the existing tables from database.
     */
    abstract fun getAvailableTables(conf: DbConfig): Map<String, List<TableName>>

    /**
     * List all visible databases by user from the config.
     */
    abstract fun listDatabases(conf: DbConfig): List<String>

    /**
     * Create the internal JDBC connection for those methods which doesn't have a <code>DbConfig</code> parameter.
     * This method could be called by many other methods in implementation.
     */
    open fun open(conf: DbConfig) {
        connection = getConnection(conf)
    }

    protected abstract fun getConnection(conf: DbConfig): Connection

    override fun close() {
        try {
            insertionMap.forEach { (table, _) -> flushRows(table) }
            connection.close()
        } catch (e: Throwable) {
            logger.error("flush cached rows failed: {}", e.message)
            e.printStackTrace()
        }
    }

    /**
     * Clear any data in given table.
     */
    abstract fun clearTable(table: TableName): Pair<Boolean, String?>

    /**
     * Drop given table.
     */
    abstract fun dropTable(tableName: String): Pair<Boolean, String?>
    open fun dropTable(table: TableName): Pair<Boolean, String?> {
        return dropTable(getFullTableName(table))
    }

    /**
     * Create quoted, escaped identity name of database.
     */
    open fun normalizeName(name: String): String {
        return if (isNormalizedName(name)) name else {
            val wrapper = getWrapperCharacter()
            "${wrapper.first}${escapeNameString(name)}${wrapper.second}"
        }
    }

    /**
     * Whether the given name is quoted, escaped on database's definition.
     */
    open fun isNormalizedName(name: String): Boolean {
        val w = getWrapperCharacter()
        return name.trim().startsWith(w.first) && name.trim().endsWith(w.second)
    }

    /**
     * Escape database identity name.
     */
    abstract fun escapeNameString(name: String): String

    /**
     * Get the wrapper character defined by database.
     */
    abstract fun getWrapperCharacter(): Pair<String, String>

    /**
     * Create quoted, escaped table name of database, including schema if supported.
     */
    open fun getFullTableName(schema: String, tableName: String): String {
        return if (schema.isBlank()) normalizeName(tableName)
        else "${normalizeName(schema)}.${normalizeName(tableName)}"
    }

    open fun getFullTableName(table: TableName): String {
        return getFullTableName(table.schema, table.tableName)
    }

    /**
     * Whether the given table exists in database by schema and name.
     */
    abstract fun isTableExists(table: TableName): Boolean

    /**
     * Create a new table on given name and column definitions.
     */
    abstract fun createTable(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>)

    /**
     * Find local database type on given JDBC type.
     */
    abstract fun convertJDBCTypeToDBNativeType(aType: JDBCType): String

    /**
     * Fetch column definitions of given table.
     */
    abstract fun getExistingTableDefinition(table: TableName): TableDefinition<in ColumnDefinition>

    /**
     * Find JDBC type on database local type.
     */
    abstract fun mapDBTypeToJDBCType(typeName: String): JDBCType

    /**
     * Whether this database is case sensitive.
     */
    abstract fun isCaseSensitive(): Boolean

    /**
     * Get default schema of database.
     */
    abstract fun getDefaultSchema(): String

    /**
     * Set the record amount of one batch insert.
     */
    open fun getInsertBatchAmount(): Long {
        return 1000
    }

    /**
     * this method is for those database / JDBC drivers that doesn't support certain kind
     * of data types, e.g. timestamp in hive.
     * It should convert a value with specific unsupported type to new value in supported
     * type, to allow database inserting functions to create a compatible JDBC statement.
     */
    open fun getTypedDataConverters(): Map<JDBCType, Pair<JDBCType, (Any?) -> Any?>> {
        return mapOf()
    }

    open fun writeRow(table: TableName, row: DataRow) {
        if (!insertionMap.contains(table)) {
            throw F2TException("Way to insert into table ${getFullTableName(table)} is unknown")
        }
        rows.add(row)
        if (rows.size >= getInsertBatchAmount()) {
            flushRows(table)
        }
    }

    open fun flushRows(table: TableName) {
        val fieldValueSetter = fieldValueSetters.getValue(table)
        //logger.debug(insertionMap.getValue(table))
        connection.prepareStatement(insertionMap.getValue(table)).use { stmt ->
            rows.forEach { row ->
                row.cells.sortedBy { it.index }.forEachIndexed { i, cell ->
                    fieldValueSetter[i].invoke(stmt, i + 1, cell.data)
                }
                stmt.addBatch()
            }
            stmt.executeBatch()
            logger.info("${rows.size} row${if (rows.size > 1) "s" else ""} inserted into table ${table}.")
        }
        rows.clear()
    }

    open fun prepareInsertion(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>) {
        val sql = """
                insert into ${getFullTableName(table)} (${tableDefinition.columns.joinToString { normalizeName(it.name) }})
                values (${tableDefinition.columns.joinToString { "?" }})
            """
        insertionMap[table] = sql
        fieldValueSetters[table] = tableDefinition.columns.sortedBy { it.name }.map { col ->
            val converter = getTypedDataConverters()[col.dataType]
            if (converter == null) {
                createFieldSetter(col.dataType!!)
            } else {
                createFieldSetter(converter.first) { it -> converter.second.invoke(it) }
            }
        }
    }

    private fun createFieldSetter(type: JDBCType, transformer: (Any?) -> Any? = { it }) = when (type) {
        JDBCType.BOOLEAN -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setBoolean(i, newValue as Boolean) else stmt.setNull(i, Types.BOOLEAN)
        }
        JDBCType.INTEGER -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setInt(i, newValue as Int) else stmt.setNull(i, Types.INTEGER)
        }
        JDBCType.BIGINT -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setLong(i, newValue as Long) else stmt.setNull(i, Types.BIGINT)
        }
        JDBCType.DECIMAL, JDBCType.FLOAT, JDBCType.DOUBLE -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setDouble(i, newValue as Double) else stmt.setNull(i, Types.DOUBLE)
        }
        JDBCType.TIMESTAMP -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setTimestamp(i, Timestamp.from((newValue as ZonedDateTime).toInstant()))
            else stmt.setNull(i, Types.TIMESTAMP)
        }
        else -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString()) else stmt.setNull(i, Types.CLOB)
        }
    }

}
