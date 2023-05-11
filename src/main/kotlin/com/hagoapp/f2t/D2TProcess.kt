/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.database.DbConnection
import com.hagoapp.f2t.database.DbConnectionFactory
import com.hagoapp.f2t.database.TableName
import com.hagoapp.f2t.datafile.ParseResult
import com.hagoapp.util.StackTraceWriter
import org.slf4j.LoggerFactory
import java.lang.reflect.Method
import java.sql.Connection

/**
 * This class implements a process from a data table object to table in database.
 *
 * @param dataTable  the source data set
 * @param conn Sql Connection
 * @param f2TConfig  the configuration of process itself
 * @author Chaojun Sun
 * @since 0.2
 */
class D2TProcess(
    private var dataTable: DataTable<FileColumnDefinition>,
    conn: Connection,
    private val f2TConfig: F2TConfig
) {
    private val connection: DbConnection
    private val logger = LoggerFactory.getLogger(D2TProcess::class.java)
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
        if (f2TConfig.isAddBatch && (f2TConfig.batchColumnName == null)) {
            logger.error("identity column can't be null when addIdentity set to true")
            throw F2TException("identity column can't be null when addIdentity set to true")
        }
        table = TableName(f2TConfig.targetTable, f2TConfig.targetSchema ?: "")
        connection = DbConnectionFactory.createDbConnection(conn)
    }

    /**
     * start process.
     */
    fun run(): ParseResult {
        connection.use {
            progressNotifier?.onStart()
            val writer = FileDataTableWriter(connection, f2TConfig, dataTable.columnDefinition)
            val parseResult = ParseResult()
            try {
                val count = dataTable.rows.size
                dataTable.rows.forEachIndexed { i, row ->
                    try {
                        writer.writeRow(row)
                        progressNotifier?.onProgress(i.toFloat() / count.toFloat())
                    } catch (e: Throwable) {
                        StackTraceWriter.writeToLogger(e, logger)
                        parseResult.addError(i.toLong(), e)
                    }
                }
                it.flushRows(table)
            } catch (e: Throwable) {
                StackTraceWriter.writeToLogger(e, logger)
                parseResult.addError(-1L, e)
            } finally {
                parseResult.end()
            }
            progressNotifier?.onComplete(parseResult)
            return parseResult
        }
    }

}
