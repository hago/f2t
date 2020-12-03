/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.database.config.DbConfig
import com.hagoapp.f2t.database.config.HiveConfig
import com.hagoapp.f2t.database.config.hive.ServiceDiscoveryMode
import java.sql.Connection
import java.sql.DriverManager
import java.sql.JDBCType
import java.sql.SQLException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class HiveConnection : DbConnection() {

    companion object {
//        private const val SERVICE_DISCOVERY_MODE = "serviceDiscoveryMode"
//        private const val ZOOKEEPER_NODES = "zookeeperNodes"
//        private const val ZOOKEEPER_NAMESPACE = "zookeeperNamespace"
        private const val HIVE_DRIVER_CLASS_NAME = "com.cloudera.hive.jdbc.HS2Driver"
        private var driverFound: Boolean? = null
    }

    private lateinit var hiveConfig: HiveConfig

    init {
        if (driverFound == null) {
            driverFound = try {
                Class.forName(HIVE_DRIVER_CLASS_NAME)
                true
            } catch (e: ClassNotFoundException) {
                false
            }
        }
        if (!driverFound!!) {
            val msg = "Cloudera hive driver '$HIVE_DRIVER_CLASS_NAME' not found, hive connection not available"
            logger.error(msg)
            throw F2TException(msg)
        }
    }

    override fun open(conf: DbConfig) {
        super.open(conf)
        hiveConfig = getHiveConfig(conf)
    }

    override fun getConnection(conf: DbConfig): Connection {
        val config = getHiveConfig(conf)
        var conStr = if (config.serviceDiscoveryMode == ServiceDiscoveryMode.ZooKeeper) createZKConnectInfo(config)
        else createHiveConnectInfo(config)
        conStr += "transportMode=${config.transportMode};AuthMech=${config.authMech.value}"
        val props = Properties()
        props.putAll(mapOf("user" to config.username, "password" to conf.password))
        logger.debug("connect hive using: $conStr")
        val connection = DriverManager.getConnection(conStr, props)
        if (config.databaseName != null) {
            setCurrentDatabase(connection, config.databaseName)
        }
        return connection
    }

    private fun getHiveConfig(conf: DbConfig): HiveConfig {
        if (conf !is HiveConfig) {
            throw F2TException("Not a hive connection config")
        }
        return conf
    }

    private fun createHiveConnectInfo(conf: HiveConfig): String {
        if (conf.databaseName.isNullOrBlank()) {
            conf.databaseName = "default"
        }
        return "jdbc:hive2://${conf.host}:${conf.port}/${conf.databaseName};"
    }

    private fun createZKConnectInfo(conf: HiveConfig): String {
        val zk = conf.quorums.joinToString(",") { node ->
            "zk=${node.host}:${node.port}/${conf.zooKeeperNamespace}"
        }
        return "jdbc:hive2://${conf.databaseName};$zk;"
    }

    override fun canConnect(conf: DbConfig): Pair<Boolean, String> {
        try {
            getConnection(conf).use {
                return Pair(true, "")
            }
        } catch (ex: Exception) {
            logger.info("connect test failed: ${ex.message}")
            return Pair(false, ex.message ?: ex.toString())
        }
    }

    override fun getAvailableTables(conf: DbConfig): Map<String, List<TableName>> {
        val config = getHiveConfig(conf)
        getConnection(conf).use { con ->
            val databases = if (config.databaseName.isNullOrBlank()) listDatabasesWithConnection(con)
            else listOf(config.databaseName)
            val ret = mutableMapOf<String, List<TableName>>()
            databases.forEach { dbName ->
                val tables = mutableListOf<TableName>()
                con.prepareStatement("show tables in ${normalizeName(dbName)}").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        while (rs.next()) {
                            val table = rs.getString(1)
                            tables.add(TableName(table))
                        }
                    }
                }
                ret[dbName] = tables
            }
            return ret
        }
    }

    override fun listDatabases(conf: DbConfig): List<String> {
        getConnection(conf).use { con ->
            return listDatabasesWithConnection(con)
        }
    }

    private fun listDatabasesWithConnection(con: Connection): List<String> {
        con.prepareStatement("show databases").use { stmt ->
            stmt.executeQuery().use { rs ->
                val ret = mutableListOf<String>()
                while (rs.next()) {
                    ret.add(rs.getString("database_name"))
                }
                return ret
            }
        }
    }

    override fun clearTable(table: TableName): Pair<Boolean, String?> {
        return try {
            connection.prepareStatement("truncate table ${getFullTableName(table)}")
                .use { stmt -> stmt.execute() }
            Pair(true, null)
        } catch (e: SQLException) {
            Pair(false, e.message)
        }
    }

    override fun dropTable(tableName: String): Pair<Boolean, String?> {
        return try {
            connection.prepareStatement("drop table $tableName").use { stmt -> stmt.execute() }
            Pair(true, null)
        } catch (e: SQLException) {
            Pair(false, e.message)
        }
    }

    override fun escapeNameString(name: String): String {
        return name.replace("`", "``")
    }

    override fun getWrapperCharacter(): Pair<String, String> {
        return Pair("`", "`")
    }

    override fun getDefaultSchema(): String {
        return ""
    }

    private fun setCurrentDatabase(connection: Connection, dbName: String) {
        connection.prepareStatement("use $dbName").use { it.execute() }
    }

    override fun isCaseSensitive(): Boolean {
        return false
    }

    override fun mapDBTypeToJDBCType(typeName: String): JDBCType {
        return when {
            typeName.compareTo("int") == 0 -> JDBCType.INTEGER
            typeName.compareTo("bigint") == 0 -> JDBCType.BIGINT
            typeName.compareTo("boolean") == 0 -> JDBCType.BOOLEAN
            typeName.startsWith("timestamp") -> JDBCType.TIMESTAMP
            typeName.startsWith("double") || typeName.startsWith("decimal") -> JDBCType.DOUBLE
            typeName.compareTo("float") == 0 -> JDBCType.DOUBLE
            else -> JDBCType.CLOB
        }
    }

    override fun getExistingTableDefinition(table: TableName): TableDefinition {
        val ftn = getFullTableName(table)
        val sql = """describe $ftn"""
        connection.prepareStatement(sql).use { stmt ->
            stmt.executeQuery().use { rs ->
                val tblColDef = mutableMapOf<String, JDBCType>()
                while (rs.next()) {
                    val col = rs.getString("col_name")
                    tblColDef[col] = mapDBTypeToJDBCType(rs.getString("data_type"))
                }
                if (tblColDef.isEmpty()) {
                    throw F2TException("Column definition of file differs from existing table $ftn")
                }
                return TableDefinition(tblColDef.entries.mapIndexed { i, col ->
                    ColumnDefinition(i, col.key, mutableSetOf(col.value), col.value)
                }.toSet())
            }
        }
    }

    override fun convertJDBCTypeToDBNativeType(aType: JDBCType): String {
        return when (aType) {
            JDBCType.BOOLEAN -> "boolean"
            JDBCType.TIMESTAMP -> "timestamp"
            JDBCType.BIGINT -> "bigint"
            JDBCType.INTEGER -> "int"
            JDBCType.DOUBLE, JDBCType.DECIMAL, JDBCType.FLOAT -> "double"
            else -> "string"
        }
    }

    override fun isTableExists(table: TableName): Boolean {
        connection.prepareStatement("show tables in ${hiveConfig.databaseName} ${getFullTableName(table)}").use { stmt ->
            stmt.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    override fun createTable(table: TableName, tableDefinition: TableDefinition) {
        val content = tableDefinition.columns.map { col ->
            "${normalizeName(col.name)} ${convertJDBCTypeToDBNativeType(col.inferredType!!)}"
        }.joinToString(", ")
        val sql = "create table ${getFullTableName(table)} ($content);"
        logger.debug("create table using SQL: $sql")
        connection.prepareStatement("use ${hiveConfig.databaseName}").use { it.execute() }
        connection.prepareStatement(sql).use { stmt ->
            stmt.execute()
        }
    }

    override fun getInsertBatchAmount(): Long {
        return 100
    }

    override fun getTypedDataConverters(): Map<JDBCType, Pair<JDBCType, (Any?) -> Any?>> {
        val lambda = Pair(
            JDBCType.CLOB,
            { value: Any? ->
                (value as ZonedDateTime).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z"))
            }
        )
        return mapOf(
            JDBCType.TIMESTAMP to lambda
        )
    }
}
