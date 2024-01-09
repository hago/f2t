/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.database.config.DerbyConfig
import com.hagoapp.f2t.database.derby.TypeParser
import java.sql.Connection
import java.sql.JDBCType

/**
 * The implementation for Apache Derby.
 *
 * @author suncjs
 * @since 0.8.5
 */
class DerbyConnection : DbConnection() {

    companion object {
        const val DERBY_TABLE_TYPE_USER_TABLE = 'T'
    }

    private var majorVersion: Int = 10
    private var minorVersion: Int = 0

    override fun open(conn: Connection) {
        super.open(conn)
        conn.prepareStatement("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('DataDictionaryVersion')").use { st ->
            st.executeQuery().use { rs ->
                rs.next()
                val parts = rs.getString(1).split('.')
                majorVersion = parts[0].toInt()
                minorVersion = if (parts.size > 1) parts[1].toInt() else 0
            }
        }
    }

    override fun getDriverName(): String {
        return DerbyConfig.JDBC_DRIVER_APACHE_DERBY;
    }

    private data class SchemaResult(
        val schemaId: String,
        val schemaName: String,
        val authorizationId: String
    )

    private data class TableResult(
        val tableId: String,
        val tableName: String,
        val tableType: Char,
        val schemaId: String,
        val lockGranularity: Char
    )

    override fun getAvailableTables(): Map<String, List<TableName>> {
        val schemas = mutableListOf<SchemaResult>()
        connection.prepareStatement("SELECT * FROM SYS.SYSSCHEMAS").use { stmt ->
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    schemas.add(
                        SchemaResult(
                            rs.getString("SCHEMAID"),
                            rs.getString("SCHEMANAME"),
                            rs.getString("AUTHORIZATIONID")
                        )
                    )
                }
            }
        }
        val tables = mutableListOf<TableResult>()
        connection.prepareStatement("SELECT * FROM SYS.SYSTABLES WHERE TABLETYPE = ?").use { st ->
            st.setString(1, DERBY_TABLE_TYPE_USER_TABLE.toString())
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    tables.add(
                        TableResult(
                            rs.getString("TABLEID"),
                            rs.getString("TABLENAME"),
                            DERBY_TABLE_TYPE_USER_TABLE,
                            rs.getString("SCHEMAID"),
                            rs.getString("LOCKGRANULARITY")[0]
                        )
                    )
                }
            }
        }
        return schemas.associate { s ->
            Pair(s.schemaName,
                tables.filter { t -> t.schemaId == s.schemaId }.map { TableName(it.tableName, s.schemaName) })
        }
    }

    override fun listDatabases(): List<String> {
        connection.prepareStatement("values SYSCS_UTIL.SYSCS_GET_DATABASE_NAME()").use { st ->
            st.executeQuery().use { rs ->
                rs.next()
                return listOf(rs.getString(1))
            }
        }
    }

    override fun escapeNameString(name: String): String {
        return name.replace("\"", "\"\"")
    }

    override fun getWrapperCharacter(): Pair<String, String> {
        return Pair("\"", "\"")
    }

    override fun isTableExists(table: TableName): Boolean {
        val sql = """
            select 1 from sys.sysschemas as sc
            inner join sys.systables as st on sc.schemaid = st.schemaid
            where sc.schemaname = ? and st.tablename = ? and st.tabletype='T'
            """
        connection.prepareStatement(sql).use { st ->
            st.setString(1, table.schema)
            st.setString(2, table.tableName)
            st.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    override fun createTable(table: TableName, tableDefinition: TableDefinition<out ColumnDefinition>) {
        val tableFullName = getFullTableName(table)
        val defStr = tableDefinition.columns.joinToString(", ") { colDef ->
            val colLine = "${normalizeName(colDef.name)} ${
                convertJDBCTypeToDBNativeType(
                    colDef.dataType,
                    colDef.typeModifier
                )
            }"
            val nullable = if (colDef.typeModifier.isNullable) "" else "NOT NULL"
            "$colLine $nullable"
        }
        val primaryKeyDef = if (tableDefinition.primaryKey?.columns == null) {
            null
        } else {
            val p = tableDefinition.primaryKey!!
            "CONSTRAINT ${normalizeName(p.name)} PRIMARY KEY (${
                p.columns.joinToString(", ") { normalizeName(it.name) }
            })"
        }
        val uniqueDef = if (tableDefinition.uniqueConstraints.isEmpty()) {
            null
        } else {
            tableDefinition.uniqueConstraints.joinToString(",") {
                val head = "CONSTRAINT ${normalizeName(it.name)} UNIQUE "
                val uniqueCols = "(${it.columns.joinToString(",") { col -> normalizeName(col.name) }})"
                head + uniqueCols
            }
        }
        var body = if (primaryKeyDef == null) defStr else "$defStr, $primaryKeyDef"
        if (uniqueDef != null) {
            body += ", $uniqueDef"
        }
        val sql = "CREATE TABLE $tableFullName ($body)"
        logger.debug("create table $tableFullName using: $sql")
        connection.prepareStatement(sql).use { it.execute() }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String {
        return when (aType) {
            JDBCType.BIGINT -> "BIGINT"
            JDBCType.BINARY, JDBCType.VARBINARY, JDBCType.LONGVARBINARY, JDBCType.CHAR,
            JDBCType.VARCHAR, JDBCType.LONGVARCHAR -> convertCharBinaryTypes(aType, modifier)

            JDBCType.BLOB -> "BLOB" + if (modifier.maxLength > 0) "(${modifier.maxLength}" else ""
            JDBCType.CLOB -> "CLOB" + if (modifier.maxLength > 0) "(${modifier.maxLength}" else ""
            JDBCType.DECIMAL -> "DECIMAL(${modifier.precision}, ${modifier.scale})"
            JDBCType.FLOAT -> "REAL"
            JDBCType.DOUBLE -> "DOUBLE"
            JDBCType.SMALLINT -> "SMALLINT"
            JDBCType.INTEGER -> "INT"
            JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE -> "TIME"
            JDBCType.TIMESTAMP, JDBCType.TIMESTAMP_WITH_TIMEZONE -> "TIMESTAMP"
            JDBCType.DATE -> "DATE"
            JDBCType.BOOLEAN -> if (minorVersion < 7) "SMALLINT" else "BOOLEAN"
            else -> {
                logger.error("type {} is not implemented, treat as text", aType)
                return "CLOB"
            }
        }
    }

    private fun convertCharBinaryTypes(aType: JDBCType, modifier: ColumnTypeModifier): String {
        val len = if (modifier.maxLength > 0) modifier.maxLength.toString() else "1"
        return when (aType) {
            JDBCType.BINARY -> "CHAR($len) FOR BIT DATA"
            JDBCType.VARBINARY, JDBCType.LONGVARBINARY -> "VARCHAR($len) FOR BIT DATA"
            JDBCType.CHAR -> "CHAR($len)"
            //JDBCType.VARCHAR, JDBCType.LONGVARCHAR
            else -> "VARCHAR($len)"
        }
    }

    private data class ColumnResult(
        val referenceId: String,
        val columnName: String,
        val columnNumber: Int,
        val columnDataType: String
    )

    override fun getExistingTableDefinition(table: TableName): TableDefinition<in ColumnDefinition> {
        val cols = mutableListOf<ColumnResult>()
        val sql = """
            SELECT c.* FROM SYS.SYSCOLUMNS AS c
            INNER JOIN SYS.SYSTABLES AS T ON c.REFERENCEID = T.TABLEID
            INNER JOIN SYS.SYSSCHEMAS AS S ON S.SCHEMAID = T.SCHEMAID
            WHERE S.SCHEMANAME = ? AND T.TABLENAME = ? ORDER BY c.COLUMNNUMBER
        """
        val columns = connection.prepareStatement(sql).use { st ->
            st.setString(1, table.schema)
            st.setString(2, table.tableName)
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    cols.add(ColumnResult(
                        rs.getString("REFERENCEID"),
                        rs.getString("COLUMNNAME"),
                        rs.getInt("COLUMNNUMBER"),
                        rs.getString("COLUMNDATATYPE")
                    ))
                }
            }
            cols.map { col ->
                val p = TypeParser.parseType(col.columnDataType)
                val def = ColumnDefinition(col.columnName, p.first)
                def.typeModifier = p.second
                def
            }
        }
        return TableDefinition(columns, isCaseSensitive())
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when (typeName) {
            "INTEGER" -> JDBCType.INTEGER
            "SMALLINT" -> JDBCType.SMALLINT
            "BIGINT" -> JDBCType.BIGINT
            "REAL", "DOUBLE" -> JDBCType.DOUBLE
            "DATE" -> JDBCType.DATE
            "TIME" -> JDBCType.TIME
            "TIMESTAMP" -> JDBCType.TIMESTAMP
            "CLOB" -> JDBCType.CLOB
            "BLOB" -> JDBCType.BLOB
            "CHAR" -> JDBCType.CHAR
            "VARCHAR" -> JDBCType.VARCHAR
            else -> mapBitTypesAndOther(typeName)
        }
    }

    private fun mapBitTypesAndOther(typeName: String): JDBCType {
        return when {
            typeName.startsWith("CHAR ()") -> JDBCType.BINARY
            typeName.startsWith("VARCHAR ()") -> JDBCType.VARBINARY
            else -> {
                logger.error("typeName {} unknown, use CLOB for it", typeName)
                JDBCType.CLOB
            }
        }
    }

    override fun isCaseSensitive(): Boolean {
        return true;
    }

    override fun getDefaultSchema(): String {
        return "APP"
    }
}