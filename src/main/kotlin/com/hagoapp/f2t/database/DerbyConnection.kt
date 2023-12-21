/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.TableDefinition
import java.sql.JDBCType

class DerbyConnection : DbConnection() {
    override fun getDriverName(): String {
        return "org.apache.derby.jdbc.EmbeddedDriver"
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
        TODO("Not yet implemented")
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType, modifier: ColumnTypeModifier): String {
        TODO("Not yet implemented")
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
        TODO("Not yet implemented")
    }

    override fun isCaseSensitive(): Boolean {
        return true;
    }

    override fun getDefaultSchema(): String {
        return "APP"
    }
}