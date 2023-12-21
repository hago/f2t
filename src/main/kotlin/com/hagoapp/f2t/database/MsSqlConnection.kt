/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.*
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.util.ColumnMatcher
import com.hagoapp.util.StackTraceWriter
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement
import microsoft.sql.DateTimeOffset
import java.sql.*
import java.sql.JDBCType.*
import java.time.ZonedDateTime
import java.util.*
import kotlin.collections.set
import kotlin.math.log10
import kotlin.math.pow
import kotlin.math.round
import kotlin.math.roundToInt

/**
 * Database operation implementation for Microsoft SQL Server.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
open class MsSqlConnection : DbConnection() {

    companion object {
        private const val MSSQL_DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    override fun getDriverName(): String {
        return MSSQL_DRIVER_CLASS_NAME
    }

    override fun getAvailableTables(): Map<String, List<TableName>> {
        try {
            val ret = mutableMapOf<String, MutableList<TableName>>()
            val sql = """
                select s.name as [schema], o.name as [table] 
                from sys.schemas as s left outer join sys.objects as o on s.schema_id = o.schema_id 
                where o.[type]='U'
                order by s.name, o.name
                """
            connection.prepareStatement(sql).use { st ->
                st.executeQuery().use { rs ->
                    while (rs.next()) {
                        val schema = rs.getString("schema")
                        val table = rs.getString("table")
                        if (!ret.containsKey(schema)) {
                            ret[schema] = mutableListOf()
                        }
                        if (table != null) {
                            ret.getValue(schema).add(TableName(table, schema))
                        }
                    }
                }
            }
            connection.prepareStatement("select * from sys.schemas where principal_id=1").use { st ->
                st.executeQuery().use { rs ->
                    while (rs.next()) {
                        val schema = rs.getString("name")
                        ret.putIfAbsent(schema, mutableListOf())
                    }
                }
            }
            return ret.ifEmpty { mapOf(getDefaultSchema() to listOf()) }
        } catch (ex: SQLException) {
            logger.error("fetch table list error: $ex")
            StackTraceWriter.writeToLogger(ex, logger)
            return mapOf()
        }
    }

    override fun listDatabases(): List<String> {
        try {
            val ret = mutableListOf<String>()
            val sql = "select name from sys.databases where name not in ('master', 'tempdb', 'msdb', 'model')"
            connection.prepareStatement(sql).use { st ->
                st.executeQuery().use { rs ->
                    while (rs.next()) {
                        ret.add(rs.getString("name"))
                    }
                    return ret
                }
            }
        } catch (ex: SQLException) {
            logger.error("fetch database list error: $ex")
            StackTraceWriter.writeToLogger(ex, logger)
            return listOf()
        }
    }

    override fun dropTable(tableName: String): Pair<Boolean, String?> {
        try {
            connection.prepareStatement("drop table ${normalizeName(tableName)};").use { st ->
                st.execute()
                return Pair(true, null)
            }
        } catch (ex: SQLException) {
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
            stmt.setString(1, table.schema.ifBlank { getDefaultSchema() })
            stmt.setString(2, table.tableName)
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
                val uniqueCols = "(${it.columns.joinToString(",") { col -> normalizeName(col.name) }})"
                head + uniqueCols
            }
        }
        content = if (primaryKeyDef == null) content else "$content, $primaryKeyDef"
        if (uniqueDef != null) {
            content += ", $uniqueDef"
        }
        val sql = "create table ${getFullTableName(table)} ($content);"
        logger.debug("create table using SQL: $sql")
        connection.prepareStatement(sql).use { stmt ->
            stmt.execute()
        }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String {
        return when (aType) {
            BOOLEAN -> "bit"
            TINYINT -> "tinyint"
            SMALLINT -> "smallint"
            INTEGER -> "int"
            BIGINT -> "bigint"
            CHAR -> "char(${modifier.maxLength})"
            NCHAR -> "nchar(${modifier.maxLength})"
            VARCHAR -> "varchar(${modifier.maxLength})"
            NVARCHAR -> "nvarchar(${modifier.maxLength})"
            CLOB -> "text"
            NCLOB -> "ntext"
            FLOAT -> "float"
            DOUBLE -> "real"
            DATE -> "date"
            TIME, TIME_WITH_TIMEZONE -> "time"
            TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> "datetimeoffset"
            DECIMAL -> "decimal(${modifier.precision}, ${modifier.scale})"
            else -> "ntext"
        }
    }

    override fun getExistingTableDefinition(table: TableName): TableDefinition<ColumnDefinition> {
        val sql = """
                select TYPE_NAME(c.system_type_id) as typeName, c.name, o.object_id,
                c.max_length, c.precision, c.scale, c.collation_name, c.is_nullable
                from  sys.schemas as s
                inner join sys.objects as o on s.schema_id = o.schema_id
                inner join sys.columns as c on o.object_id = c.object_id
                where o.name=? and o.type='U' and s.name = ?
                order by c.column_id
            """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            stmt.setString(1, table.tableName)
            val schema = table.schema.ifBlank { getDefaultSchema() }
            stmt.setString(2, schema)
            stmt.executeQuery().use { rs ->
                val tblColDef = mutableListOf<ColumnDefinition>()
                while (rs.next()) {
                    val name = rs.getNString("name")
                    val typeName = rs.getNString("typeName")
                    val type = mapDBTypeToJDBCType(typeName)
                    val colDef = ColumnDefinition(name, type)
                    colDef.databaseTypeName = typeName
                    colDef.typeModifier.maxLength = rs.getInt("max_length")
                    colDef.typeModifier.precision = rs.getInt("precision")
                    colDef.typeModifier.scale = rs.getInt("scale")
                    colDef.typeModifier.collation = rs.getString("collation_name")
                    colDef.typeModifier.isNullable = rs.getBoolean("is_nullable")
                    tblColDef.add(colDef)
                }
                if (tblColDef.isEmpty()) {
                    throw SQLException(
                        "Column definition of table ${getFullTableName(schema, table.tableName)} not found"
                    )
                }
                val ret = TableDefinition(tblColDef, isCaseSensitive())
                val uqKeys = findPrimaryKeyAndUniques(table, ret.columns)
                ret.primaryKey = uqKeys.first
                ret.uniqueConstraints = uqKeys.second
                return ret
            }
        }
    }

    private fun getUniqueKeyConstraints(table: TableName, type: String): Map<String, List<String>> {
        val sql = """SELECT kc.type, i.index_id, kc.name as index_name, c.name as column_name
                from sys.objects as o
                inner join sys.indexes as i on o.object_id = i.object_id 
                inner join sys.index_columns as ic on o.object_id = ic.object_id and i.index_id = ic.index_id 
                inner join sys.key_constraints as kc on i.name = kc.name 
                inner join sys.columns as c on c.object_id = o.object_id and c.column_id = ic.column_id 
                inner join sys.schemas as s on s.schema_id = o.schema_id 
                where o.name = ? and s.name = ? and kc.type = ?
                order by ic.index_column_id"""
        val ret: MutableMap<String, MutableList<String>> = HashMap()
        try {
            connection.prepareStatement(sql).use { st ->
                st.setString(1, table.tableName)
                st.setString(2, table.schema)
                st.setString(3, type)
                st.executeQuery().use { rs ->
                    while (rs.next()) {
                        val keyName = rs.getNString("index_name")
                        val colName = rs.getNString("column_name")
                        if (!ret.containsKey(keyName)) {
                            ret[keyName] = ArrayList()
                        }
                        ret.getValue(keyName).add(colName)
                    }
                }
                return ret
            }
        } catch (e: SQLException) {
            throw IllegalStateException("Table definition query error", e)
        }
    }

    private fun findPrimaryKeyAndUniques(
        table: TableName, refColumns: List<ColumnDefinition>
    ): Pair<TableUniqueDefinition<ColumnDefinition>?, Set<TableUniqueDefinition<ColumnDefinition>>> {
        val pk = fromColumns(getUniqueKeyConstraints(table, "PK"), refColumns).firstOrNull()
        val uqs = fromColumns(getUniqueKeyConstraints(table, "UQ"), refColumns).toSet()
        return Pair(pk, uqs)
    }

    private fun fromColumns(
        uniques: Map<String, List<String>>,
        refColumns: List<ColumnDefinition>
    ): List<TableUniqueDefinition<ColumnDefinition>> {
        if (uniques.isEmpty()) {
            return listOf()
        }
        val colMatcher = ColumnMatcher.getColumnMatcher(isCaseSensitive())
        val ret = mutableListOf<TableUniqueDefinition<ColumnDefinition>>()
        for ((keyName, columns) in uniques) {
            val colDef = columns.map { col -> refColumns.first { refCol -> colMatcher.invoke(col, refCol.name) } }
            val unique = TableUniqueDefinition(keyName, colDef, isCaseSensitive())
            ret.add(unique)
        }
        return ret
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when {
            typeName.startsWith("int") -> INTEGER
            typeName == "tinyint" -> TINYINT
            typeName == "smallint" -> SMALLINT
            typeName == "uniqueidentifier" -> CHAR
            typeName == "bigint" -> BIGINT
            typeName == "bit" -> BOOLEAN
            setOf("smallmoney", "money", "numeric").contains(typeName) -> NUMERIC
            typeName == "datetimeoffset" -> TIMESTAMP_WITH_TIMEZONE
            typeName == "datetime" || typeName == "smalldatetime" || typeName == "datetime2" -> TIMESTAMP
            typeName == "time" -> TIME
            typeName == "date" -> DATE
            typeName == "float" -> FLOAT
            typeName == "real" -> DOUBLE
            typeName.startsWith("decimal") -> DECIMAL
            typeName == "char" -> CHAR
            typeName == "nchar" -> NCHAR
            typeName == "varchar" -> VARCHAR
            typeName == "nvarchar" -> NVARCHAR
            typeName == "text" -> CLOB
            typeName == "ntext" -> NCLOB
            typeName == "binary" -> BINARY
            typeName == "varbinary" -> VARBINARY
            typeName == "image" -> VARBINARY
            else -> throw UnsupportedOperationException("Unsupported mssql type: $typeName")
        }
    }

    private var caseSensitive: Boolean? = null
    override fun isCaseSensitive(): Boolean {
        if (caseSensitive == null) {
            val sql =
                "select convert(varchar, DATABASEPROPERTYEX('${connection.catalog}', 'collation')) as SQLCollation"
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

    override fun prepareInsertion(
        fileDefinition: TableDefinition<FileColumnDefinition>,
        table: TableName,
        tableDefinition: TableDefinition<out ColumnDefinition>
    ) {
        super.prepareInsertion(fileDefinition, table, tableDefinition)
        val columnMatcher = ColumnMatcher.getColumnMatcher(tableDefinition.caseSensitive)
        val existingSetters = fieldValueSetters.getValue(table)
        fieldValueSetters[table] = existingSetters.mapIndexed { index, setter ->
            val fileCol = fileDefinition.columns.first { it.order == index }
            val dbCol = tableDefinition.columns.first { columnMatcher(it.name, fileCol.name) }
            val transformer = ColumnComparator.getTransformer(fileCol, dbCol)
            when (dbCol.databaseTypeName) {
                "datetimeoffset" -> object : DbFieldSetter() {
                    override fun setValueForFieldIndex(stmt: PreparedStatement, i: Int, value: Any?) {
                        val newValue = this.transformer.transform(value)
                        if (newValue != null) {
                            val st = stmt as SQLServerPreparedStatement
                            val ts = newValue as ZonedDateTime
                            val dto =
                                DateTimeOffset.valueOf(Timestamp.from(ts.toInstant()), GregorianCalendar.getInstance())
                            st.setDateTimeOffset(i, dto)
                            logger.warn("SQL Server datetimeoffset")
                        } else stmt.setNull(i, Types.CHAR)
                    }
                }.withTransformer { src -> transformer.transform(src, fileCol, dbCol) }

                else -> setter
            }
        }
    }

    override fun readData(table: TableName, columns: List<ColumnDefinition>, limit: Int): List<List<Any?>> {
        val sqlBuilder = StringBuilder()
        val validColumns = columns.ifEmpty { getExistingTableDefinition(table).columns }
        val columnSelection = validColumns.joinToString(", ") { normalizeName(it.name) }
        sqlBuilder.append("select top $limit ").append(columnSelection).append(" from ").append(getFullTableName(table))
        val ret = mutableListOf<List<Any?>>()
        println(sqlBuilder)
        connection.prepareStatement(sqlBuilder.toString()).use { stmt ->
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    val row = validColumns.map { col ->
                        createDataGetter(col.dataType).getTypedValue(rs, col.name)
                    }
                    ret.add(row)
                }
                return ret
            }
        }
    }

    override fun createDataGetter(jdbcType: JDBCType): DbDataGetter<*> {
        return when (jdbcType) {
            FLOAT -> DbDataGetter { resultSet: ResultSet, column: String ->
                val v = resultSet.getDouble(column)
                val digits = log10(v).roundToInt()
                if (digits >= 15) v
                else {
                    val factor = (10.0).pow(15 - digits)
                    round(v * factor) / factor
                }
            }

            else -> super.createDataGetter(jdbcType)
        }
    }
}
