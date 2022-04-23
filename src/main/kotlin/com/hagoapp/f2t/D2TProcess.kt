/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.compare.TableDefinitionComparator
import com.hagoapp.f2t.database.DbConnection
import com.hagoapp.f2t.database.DbConnectionFactory
import com.hagoapp.f2t.database.TableName
import com.hagoapp.f2t.datafile.ParseResult
import com.hagoapp.util.StackTraceWriter
import java.lang.reflect.Method
import java.sql.Connection
import java.sql.JDBCType
import java.time.Instant

/**
 * This class implements a process from a data table object to table in database.
 *
 * @property dataTable  the source data set
 * @param dbConfig   the config of target database and table
 * @param f2TConfig  the configuration of process itself
 * @author Chaojun Sun
 * @since 0.2
 */
class D2TProcess(private var dataTable: DataTable<FileColumnDefinition>, conn: Connection, f2TConfig: F2TConfig) {
    private val connection: DbConnection
    private var config: F2TConfig = f2TConfig
    private val logger = F2TLogger.getLogger()
    private var tableMatchedFile = false
    private val table: TableName

    /**
     * the observer of this process
     */
    var progressNotifier: ProgressNotify? = null

    companion object {
        private val methods = mutableMapOf<String, Method>()

        init {
            for (method in ParseObserver::class.java.declaredMethods) {
                methods[method.name] = method
            }
        }
    }

    init {
        if (config.isAddBatch && (config.batchColumnName == null)) {
            logger.error("identity column can't be null when addIdentity set to true")
            throw F2TException("identity column can't be null when addIdentity set to true")
        }
        table = TableName(config.targetTable, config.targetSchema ?: "")
        connection = DbConnectionFactory.createDbConnection(conn)
    }

    /**
     * start process.
     */
    fun run(): ParseResult {
        progressNotifier?.onStart()
        val parseResult = ParseResult()
        try {
            if (!prepareTable()) {
                throw F2TException("DataTable object doesn't match existing table $table in database, or new table creation is forbidden.")
            }
            val count = dataTable.rows.size
            dataTable.rows.forEachIndexed { i, row ->
                try {
                    onRowRead(row)
                    progressNotifier?.onProgress(i.toFloat() / count.toFloat())
                } catch (e: Throwable) {
                    StackTraceWriter.writeToLogger(e, logger)
                    parseResult.addError(i.toLong(), e)
                }
            }
            connection.flushRows(table)
        } catch (e: Throwable) {
            StackTraceWriter.writeToLogger(e, logger)
            parseResult.addError(-1L, e)
        } finally {
            parseResult.end()
        }
        progressNotifier?.onComplete(parseResult)
        return parseResult
    }

    private fun prepareTable(): Boolean {
        if (config.isAddBatch && !dataTable.columnDefinition.any { it.name == config.batchColumnName }) {
            val batchIndex = dataTable.columnDefinition.size
            val batchColDef = FileColumnDefinition(config.batchColumnName, setOf(JDBCType.BIGINT), JDBCType.BIGINT)
            val newDefinitions = dataTable.columnDefinition.toMutableList().plus(batchColDef)
            val batchNumber = Instant.now().toEpochMilli()
            val rows = dataTable.rows.map { dataRow ->
                DataRow(dataRow.rowNo, dataRow.cells.toMutableList().plus(DataCell(batchNumber, batchIndex)))
            }
            dataTable = DataTable(newDefinitions, rows)
        }
        if (connection.isTableExists(table)) {
            val tblDef = connection.getExistingTableDefinition(table)
            //val difference = tblDef.diff(dataTable.columnDefinition.toSet())
            val difference = TableDefinitionComparator.compare(dataTable.columnDefinition.toSet(), tblDef)
            return if (!difference.isOfSameSchema()) {
                logger.error("table $table existed and differ from data to be imported, all follow-up database actions aborted")
                logger.error(difference.toString())
                false
            } else if (!difference.isIdentical()) {
                logger.error("table $table existed and varies in data definition, data may loss while writing")
                logger.error(difference.toString())
                false
            } else {
                tableMatchedFile = true
                if (config.isClearTable) {
                    connection.clearTable(table)
                    logger.warn("table ${connection.getFullTableName(table)} cleared")
                }
                connection.prepareInsertion(TableDefinition(dataTable.columnDefinition.toSet()), table, tblDef)
                logger.info("table $table found and matches")
                true
            }
        } else {
            if (config.isCreateTableIfNeeded) {
                val tblDef = TableDefinition(dataTable.columnDefinition.toSet())
                return try {
                    connection.createTable(table, tblDef)
                    connection.prepareInsertion(TableDefinition(dataTable.columnDefinition.toSet()), table, tblDef)
                    tableMatchedFile = true
                    logger.info("table $table created")
                    true
                } catch (e: Throwable) {
                    logger.error("Error occurs when creating table $table: ${e.message}")
                    false
                }
            } else {
                logger.error("table $table not existed and auto creation is not enabled, all follow-up database actions aborted")
                return false
            }
        }
    }

    private fun onRowRead(row: DataRow) {
        if (tableMatchedFile) {
            connection.writeRow(table, row)
        }
    }

}
