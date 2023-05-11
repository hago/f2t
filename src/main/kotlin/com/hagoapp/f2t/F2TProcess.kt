/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.database.DbConnection
import com.hagoapp.f2t.database.DbConnectionFactory
import com.hagoapp.f2t.database.TableName
import com.hagoapp.f2t.datafile.FileInfo
import com.hagoapp.f2t.datafile.ParseResult
import com.hagoapp.f2t.util.ColumnMatcher
import org.slf4j.LoggerFactory
import java.lang.reflect.Method
import java.sql.Connection

/**
 * This class implements a process that extract data from data file, create according table(if necessary, based on
 * the config) and write data into it. It may add one additional column of timestamp in long integer to identify
 * different running(based on config), or truncate existing data from table(based on config).
 *
 * @author Chaojun Sun
 * @since 0.1
 */
class F2TProcess(dataFileParser: FileParser, conn: Connection, private val f2TConfig: F2TConfig) : ParseObserver {
    private var parser: FileParser = dataFileParser
    private val connection: DbConnection
    private val logger = LoggerFactory.getLogger(F2TProcess::class.java)
    private val table: TableName
    private val colMatcher: (String, String) -> Boolean
    private lateinit var writer: FileDataTableWriter

    /**
     * Execution result of ths process.
     */
    val result = F2TResult()

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
        colMatcher = ColumnMatcher.getColumnMatcher(connection.isCaseSensitive())
    }

    /**
     * start process.
     */
    fun run() {
        connection.use {
            parser.addObserver(this)
            parser.parse()
        }
    }

    override fun onColumnTypeDetermined(columnDefinitionList: List<FileColumnDefinition>) {
        writer = FileDataTableWriter(connection, f2TConfig, columnDefinitionList)
    }

    override fun onRowRead(row: DataRow) {
        try {
            writer.writeRow(row)
        } catch (e: Exception) {
            result.errors.add(e)
        }
    }

    override fun onParseComplete(fileInfo: FileInfo, result: ParseResult) {
        connection.flushRows(table)
        result.end()
        this.result.complete(result)
    }

    override fun onRowError(e: Throwable): Boolean {
        result.errors.add(e)
        return true
    }

    override fun onError(e: Throwable) {
        result.errors.add(e)
    }

}
