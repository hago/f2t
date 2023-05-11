/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.*
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.database.fieldsetter.*
import com.hagoapp.f2t.util.ColumnMatcher
import com.hagoapp.util.StackTraceWriter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.sql.Connection
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Interface of database operations required to insert a set of 2-dimensional data into it.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
abstract class DbConnection : Closeable {

    protected lateinit var connection: Connection
    protected val insertionMap = mutableMapOf<TableName, String>()
    protected val rows = mutableListOf<DataRow>()
    protected val logger: Logger = LoggerFactory.getLogger(DbConnection::class.java)
    protected val fieldValueSetters = mutableMapOf<TableName, List<DbFieldSetter>>()
    val extraProperties = mutableMapOf<String, Any>()

    /**
     * Help database factory to what kind of DbConfig should lead to the implementation.
     *
     * @return the database identity string
     */
    abstract fun getDriverName(): String

    /**
     * Fetch the existing tables from database.
     *
     * @return a map whose keys are schemas and values are table names under the schema
     */
    abstract fun getAvailableTables(): Map<String, List<TableName>>

    /**
     * List all visible databases by user from the config.
     *
     * @return name list of databases attached on the instance specified by config
     */
    abstract fun listDatabases(): List<String>

    /**
     * Create the internal JDBC connection for those methods which doesn't have a <code>DbConfig</code> parameter.
     * This method could be called by many other methods in implementation.
     *
     * @param conn  database connection
     */
    open fun open(conn: Connection) {
        connection = conn
    }

    override fun close() {
        try {
            insertionMap.forEach { (table, _) -> flushRows(table) }
        } catch (e: Throwable) {
            logger.error("flush cached rows failed: {}", e.message)
            StackTraceWriter.writeToLogger(e, logger)
        }
    }

    /**
     * Clear any data in given table, descendant class should implement the operation.
     *
     * @param table table name
     * @return a pair, first element is true if clearance is successful with second element is null; otherwise, first
     * element is false and second is the error message
     */
    abstract fun clearTable(table: TableName): Pair<Boolean, String?>

    /**
     * Drop given table, descendant class should implement the operation.
     *
     * @param tableName full table name as one string
     * @return a pair, first element is true if clearance is successful with second element is null; otherwise, first
     * element is false and second is the error message
     */
    abstract fun dropTable(tableName: String): Pair<Boolean, String?>

    /**
     * Drop given table.
     *
     * @param table name
     * @return a pair, first element is true if clearance is successful with second element is null; otherwise, first
     * element is false and second is the error message
     */
    open fun dropTable(table: TableName): Pair<Boolean, String?> {
        return dropTable(getFullTableName(table))
    }

    /**
     * Create quoted, escaped identity name of database.
     *
     * @param name raw identity name
     * @return normalized name that can be recognized by database and won't be conflicted from reserved words
     */
    open fun normalizeName(name: String): String {
        return if (isNormalizedName(name)) name else {
            val wrapper = getWrapperCharacter()
            "${wrapper.first}${escapeNameString(name)}${wrapper.second}"
        }
    }

    /**
     * Whether the given name is quoted, escaped on database's definition.
     *
     * @param name  identity name
     * @return true if name is escaped to normalize form, otherwise false
     */
    open fun isNormalizedName(name: String): Boolean {
        val w = getWrapperCharacter()
        return name.trim().startsWith(w.first) && name.trim().endsWith(w.second)
    }

    /**
     * Escape database identity name.
     *
     * @param name  raw name
     * @return escaped name
     */
    abstract fun escapeNameString(name: String): String

    /**
     * Get the wrapper character defined by database.
     *
     * @return a pair containing left and right wrapper char
     */
    abstract fun getWrapperCharacter(): Pair<String, String>

    /**
     * Create quoted, escaped table name of database, including schema if supported.
     *
     * @param schema    schema name
     * @param tableName table name
     * @return full table name including schema and table
     */
    open fun getFullTableName(schema: String, tableName: String): String {
        return if (schema.isBlank()) normalizeName(tableName)
        else "${normalizeName(schema)}.${normalizeName(tableName)}"
    }

    /**
     * Create quoted, escaped table name of database, including schema if supported.
     *
     * @param table    table definition object
     * @return full table name including schema and table
     */
    open fun getFullTableName(table: TableName): String {
        return getFullTableName(table.schema, table.tableName)
    }

    /**
     * Whether the given table exists in database by schema and name.
     *
     * @param table table name
     * @return true if table exists, otherwise false
     */
    abstract fun isTableExists(table: TableName): Boolean

    /**
     * Create a new table on given name and column definitions.
     *
     * @param table table name
     * @param tableDefinition   definition of table
     */
    abstract fun createTable(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>)

    /**
     * Find corresponding database type on given JDBC type.
     *
     * @param aType JDBC type
     * @param modifier type modifier parsed from file data
     * @return database type
     */
    abstract fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String

    /**
     * Fetch column definitions of given table.
     *
     * @param table table name
     * @return table definition
     */
    abstract fun getExistingTableDefinition(table: TableName): TableDefinition<in ColumnDefinition>

    /**
     * Find JDBC type on database local type.
     *
     * @param typeName database type name
     * @return JDBC type
     */
    abstract fun mapDBTypeToJDBCType(typeName: String): JDBCType

    /**
     * Whether this database is case-sensitive.
     *
     * @return true if case-sensitive, otherwise false
     */
    abstract fun isCaseSensitive(): Boolean

    /**
     * Get default schema of database.
     *
     * @return the default schema name
     */
    abstract fun getDefaultSchema(): String

    /**
     * Set the record amount of one batch insert.
     *
     * @return a number, insertion will be performed in a batch of this amount
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

    /**
     * Write a row to table. The actual writing won't happen at once, instead, it will be triggered once
     * batch limit is reached.
     *
     * @param table table name
     * @param row   data row
     */
    open fun writeRow(table: TableName, row: DataRow) {
        if (!insertionMap.contains(table)) {
            throw F2TException("Way to insert into table ${getFullTableName(table)} is unknown")
        }
        rows.add(row)
        if (rows.size >= getInsertBatchAmount()) {
            flushRows(table)
        }
    }

    /**
     * This method should be called when batch limit is reached, it will flush internal write buffer to database.
     *
     * @param table table name
     */
    open fun flushRows(table: TableName) {
        val fieldValueSetter = fieldValueSetters[table] ?: return
        //logger.debug(insertionMap.getValue(table))
        val def = getExistingTableDefinition(table)
        connection.prepareStatement(insertionMap.getValue(table)).use { stmt ->
            rows.forEach { row ->
                row.cells.sortedBy { it.index }.forEachIndexed { i, cell ->
                    val d = def.columns[i] as ColumnDefinition
                    logger.debug(
                        "writing column {} {} with {}:{}",
                        i,
                        d.name,
                        cell.data,
                        cell.data?.javaClass?.canonicalName
                    )
                    fieldValueSetter[i].set(stmt, i + 1, cell.data)
                }
                stmt.addBatch()
            }
            stmt.executeBatch()
            logger.info("${rows.size} row${if (rows.size > 1) "s" else ""} inserted into table ${table}.")
        }
        rows.clear()
    }

    /**
     * Create an insertion SQL template for later insertion, and prepare mapping from file column to database
     * column,
     *
     * @param fileDefinition    definition of data file
     * @param table table name
     * @param tableDefinition   definition if database table
     */
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
        fieldValueSetters[table]!!.forEachIndexed { i, func ->
            logger.debug("col {} {} setter: {}", i, sortedColumns[i].name, func::class.java)
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

    private fun createFieldSetter(type: JDBCType, transformer: DataTransformer) = when (type) {
        BOOLEAN -> BooleanFieldSetter()
        TINYINT -> TinyIntFieldSetter()
        SMALLINT -> SmallIntFieldSetter()
        INTEGER -> IntegerFieldSetter()
        BIGINT -> BigIntFieldSetter()
        DECIMAL -> DecimalFieldSetter()
        FLOAT -> FloatFieldSetter()
        DOUBLE -> DoubleFieldSetter()
        TIMESTAMP_WITH_TIMEZONE, TIMESTAMP -> TimestampFieldSetter()
        DATE -> DateFieldSetter()
        TIME, TIME_WITH_TIMEZONE -> TimeFieldSetter()
        CHAR -> CharFieldSetter()
        NCHAR -> NCharFieldSetter()
        VARCHAR -> VarcharFieldSetter()
        NVARCHAR -> NVarcharFieldSetter()
        CLOB -> ClobFieldSetter()
        NCLOB -> NClobFieldSetter()
        BINARY -> BinaryFieldSetter()
        VARBINARY -> VarBinaryFieldSetter()
        else -> BigIntFieldSetter()
    }.withTransformer(transformer)

    /**
     * Read data from given table.
     *
     * @param table table name
     * @param columns   columns needs to fetch
     * @param limit the most count of fetched rows
     */
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
