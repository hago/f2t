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
import java.sql.JDBCType

class DerbyConnection : DbConnection() {
    override fun getDriverName(): String {
        return DerbyConfig.JDBC_DRIVER_APACHE_DERBY;
    }

    override fun getAvailableTables(): Map<String, List<TableName>> {
        val ret = mutableMapOf<String, MutableList<TableName>>()
        connection.prepareStatement("show schemas").use { stmt ->
            stmt.executeQuery().use { rs ->
                while (rs.next()) {
                    ret[rs.getString(1)] = mutableListOf()
                }
            }
        }
        connection.prepareStatement("show tables").use { st ->
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    val schema = rs.getString(1)
                    ret.getValue(schema).add(TableName(rs.getString(2), schema))
                }
            }
        }
        return ret
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
            val nullable = if (colDef.typeModifier.isNullable) "null" else "not null"
            "$colLine $nullable"
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
        var body = if (primaryKeyDef == null) defStr else "$defStr, $primaryKeyDef"
        if (uniqueDef != null) {
            body += ", $uniqueDef"
        }
        val sql = "create table $tableFullName ($body)"
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

    override fun getExistingTableDefinition(table: TableName): TableDefinition<in ColumnDefinition> {
        val tableString = "${table.schema}.${table.tableName}".replace("'", "''")
        val sql = "describe '$tableString'"
        val columns = mutableListOf<ColumnDefinition>()
        connection.prepareStatement(sql).use { st ->
            st.executeQuery().use { rs ->
                while (rs.next()) {
                    val t = mapDBTypeToJDBCType(rs.getString("TYPE_NAME"))
                    val col = ColumnDefinition(rs.getString("COLUMN_NAME"), t)
                    val modifier = ColumnTypeModifier()
                    col.typeModifier = modifier
                    columns.add(col)
                }
            }
            return TableDefinition(columns, isCaseSensitive())
        }
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