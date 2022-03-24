/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.F2TLogger
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.util.ColumnMatcher
import org.slf4j.Logger
import java.io.Closeable
import java.math.BigDecimal
import java.sql.*
import java.sql.JDBCType.*
import java.time.*

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
    abstract fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String

    /**
     * Fetch column definitions of given table.
     */
    abstract fun getExistingTableDefinition(table: TableName): TableDefinition<in ColumnDefinition>

    /**
     * Find JDBC type on database local type.
     */
    abstract fun mapDBTypeToJDBCType(typeName: String): JDBCType

    /**
     * Whether this database is case-sensitive.
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

    open fun prepareInsertion(
        fileDefinition: TableDefinition<FileColumnDefinition>,
        table: TableName,
        tableDefinition: TableDefinition<out ColumnDefinition>
    ) {
        val sortedColumns = sortColumnsOnFileOrder(fileDefinition, tableDefinition)
        val sql = """
                insert into ${getFullTableName(table)} (${sortedColumns.joinToString { normalizeName(it.name) }})
                values (${sortedColumns.joinToString { "?" }})
            """
        insertionMap[table] = sql
        val colMatcher = ColumnMatcher.getColumnMatcher(tableDefinition.caseSensitive)
        fieldValueSetters[table] = sortedColumns.map { col ->
            val converter = getTypedDataConverters()[col.dataType]
            if (converter == null) {
                val srcColumnDefinition = fileDefinition.columns.firstOrNull { colMatcher(it.name, col.name) }
                    ?: throw F2TException("no source for ${col.name}")
                val transformer = ColumnComparator.getTransformer(srcColumnDefinition, col)
                createFieldSetter(col.dataType) { transformer.transform(it, srcColumnDefinition, col) }
            } else {
                createFieldSetter(converter.first) { converter.second.invoke(it) }
            }
        }
    }

    private fun sortColumnsOnFileOrder(
        fileDefinition: TableDefinition<FileColumnDefinition>,
        tableDefinition: TableDefinition<out ColumnDefinition>
    ): List<ColumnDefinition> {
        val colMatcher = ColumnMatcher.getColumnMatcher(tableDefinition.caseSensitive)
        return fileDefinition.columns.sortedBy { it.order }.map { fileCol ->
            tableDefinition.columns.first { dbCol -> colMatcher(dbCol.name, fileCol.name) }
        }
    }

    private fun createFieldSetter(type: JDBCType, transformer: (Any?) -> Any? = { it }) = when (type) {
        BOOLEAN -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setBoolean(i, newValue as Boolean) else stmt.setNull(i, Types.BOOLEAN)
        }
        TINYINT -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setByte(i, newValue as Byte) else stmt.setNull(i, Types.TINYINT)
        }
        SMALLINT -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setShort(i, newValue as Short) else stmt.setNull(i, Types.SMALLINT)
        }
        INTEGER -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setInt(i, newValue as Int) else stmt.setNull(i, Types.INTEGER)
        }
        BIGINT -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setLong(i, newValue as Long) else stmt.setNull(i, Types.BIGINT)
        }
        DECIMAL -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setBigDecimal(i, newValue as BigDecimal) else stmt.setNull(i, Types.DECIMAL)
        }
        FLOAT -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setFloat(i, newValue as Float) else stmt.setNull(i, Types.FLOAT)
        }
        DOUBLE -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setDouble(i, newValue as Double) else stmt.setNull(i, Types.DOUBLE)
        }
        TIMESTAMP_WITH_TIMEZONE, TIMESTAMP -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setTimestamp(i, Timestamp.from((newValue as ZonedDateTime).toInstant()))
            else stmt.setNull(i, Types.TIMESTAMP_WITH_TIMEZONE)
        }
        DATE -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setDate(i, Date.valueOf(newValue as LocalDate))
            else stmt.setNull(i, Types.DATE)
        }
        TIME, TIME_WITH_TIMEZONE -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setTime(i, Time.valueOf(newValue as LocalTime))
            else stmt.setNull(i, Types.TIME_WITH_TIMEZONE)
        }
        CHAR -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString())
            else stmt.setNull(i, Types.CHAR)
        }
        NCHAR -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString())
            else stmt.setNull(i, Types.NCHAR)
        }
        VARCHAR -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString())
            else stmt.setNull(i, Types.VARCHAR)
        }
        NVARCHAR -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString())
            else stmt.setNull(i, Types.NVARCHAR)
        }
        CLOB -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString())
            else stmt.setNull(i, Types.CLOB)
        }
        NCLOB -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString())
            else stmt.setNull(i, Types.NCLOB)
        }
        else -> { stmt: PreparedStatement, i: Int, value: Any? ->
            val newValue = transformer.invoke(value)
            if (newValue != null) stmt.setString(i, newValue.toString()) else stmt.setNull(i, Types.CLOB)
        }
    }

    open fun readData(
        table: TableName,
        columns: List<ColumnDefinition> = listOf(),
        limit: Int = 100
    ): List<List<Any?>> {
        throw UnsupportedOperationException(
            """
            method "readData(table: TableName, columns: List<String> = listOf(), limit: Int = 100): List<List<Any?>>"
            not implemented in ${this::class.java.canonicalName}
            """
        )
    }

    protected open fun createDataGetter(jdbcType: JDBCType): DbDataGetter<*> {
        return when (jdbcType) {
            CHAR, VARCHAR, CLOB -> DbDataGetter.StringDataGetter
            NCHAR, NVARCHAR, NCLOB -> DbDataGetter.NStringDataGetter
            INTEGER -> DbDataGetter.IntDataGetter
            TINYINT -> DbDataGetter.TinyIntDataGetter
            SMALLINT -> DbDataGetter.ShortDataGetter
            BIGINT -> DbDataGetter.LongDataGetter
            FLOAT -> DbDataGetter.FloatDataGetter
            DOUBLE -> DbDataGetter.DoubleDataGetter
            DECIMAL -> DbDataGetter.DecimalDataGetter
            BOOLEAN -> DbDataGetter.BooleanDataGetter
            TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> DbDataGetter.TimestampDataGetter
            DATE -> DbDataGetter.DateDataGetter
            TIME, TIME_WITH_TIMEZONE -> DbDataGetter.TimeDataGetter
            BINARY, VARBINARY -> DbDataGetter.BINARYDataGetter
            else -> DbDataGetter.StringDataGetter
        }
    }
}
