/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.compare.TableDefinitionComparator
import com.hagoapp.f2t.database.DbConnection
import com.hagoapp.f2t.database.TableName
import com.hagoapp.f2t.util.ColumnMatcher
import org.slf4j.LoggerFactory
import java.sql.JDBCType
import java.time.Instant

/**
 * This class implements a process from a defined file source to write data into table in database.
 *
 * @property connection  database connection
 * @property f2TConfig  the configuration of process itself
 * @property fileColumnDefinitions definition list of src columns
 *
 * @author Chaojun Sun
 * @since 0.8.2
 */
internal class FileDataTableWriter(
    private val connection: DbConnection,
    private val f2TConfig: F2TConfig,
    private val fileColumnDefinitions: List<FileColumnDefinition>
) {

    private var rowCount = 0

    // -1 means either batch column is not required to add or source data already contains a batch column
    private var batchNumber = -1L
    private val logger = LoggerFactory.getLogger(FileDataTableWriter::class.java)
    private val table = TableName(f2TConfig.targetTable, f2TConfig.targetSchema ?: "")
    private val columnMatcher = ColumnMatcher.getColumnMatcher(connection.isCaseSensitive())
    private val srcColumnDefinitions: List<FileColumnDefinition> = prepareSrcDefinition()
    private val tableDefinition: TableDefinition<ColumnDefinition>
    private val fromSrcColumnMapper: List<Int>

    init {
        tableDefinition = findExpectedTableDefinition()
        fromSrcColumnMapper = tableDefinition.columns.mapIndexed { _, item ->
            val j = srcColumnDefinitions.indexOfFirst { columnMatcher.invoke(item.name, it.name) }
            if (j < 0) {
                throw F2TException("column ${item.name} in target table $table not found from source")
            }
            j
        }
        prepare()
    }

    private fun prepareSrcDefinition(): List<FileColumnDefinition> {
        return when {
            !f2TConfig.isAddBatch -> fileColumnDefinitions
            fileColumnDefinitions.any {
                columnMatcher.invoke(
                    it.name,
                    f2TConfig.batchColumnName
                )
            } -> fileColumnDefinitions

            else -> {
                val batchCol = FileColumnDefinition(
                    f2TConfig.batchColumnName,
                    setOf(JDBCType.BIGINT),
                    JDBCType.BIGINT
                )
                batchCol.order = fileColumnDefinitions.size
                batchNumber = Instant.now().toEpochMilli()
                logger.error("batch: {}", batchNumber)
                fileColumnDefinitions.plus(batchCol)
            }
        }
    }

    private fun findExpectedTableDefinition(): TableDefinition<ColumnDefinition> {
        return if (connection.isTableExists(table)) {
            // if table existed
            logger.debug("table existed, check compatibility")
            val td = connection.getExistingTableDefinition(table)
            val difference = TableDefinitionComparator.compare(srcColumnDefinitions, td)
            if (!difference.isOfSameSchema()) {
                logger.error("table {} existed and differ from file data, importing action aborted", table)
                logger.error(difference.toString())
                throw F2TException("table $table existed with different columns")
            } else if (!difference.isIdentical()) {
                logger.error("table {} existed and varies in data definition, importing action aborted", table)
                logger.error(difference.toString())
                throw F2TException("table $table existed with columns not matched")
            } else {
                logger.info("table {} found and matches", table)
                if (f2TConfig.isClearTable) {
                    connection.clearTable(table)
                    logger.warn("table ${connection.getFullTableName(table)} cleared")
                }
            }
            td
        } else {
            // table not existed, use definitions of file data to create table
            logger.debug("table not existed")
            if (f2TConfig.createTableIfNeeded) {
                val tableDef = TableDefinition(srcColumnDefinitions)
                connection.createTable(table, tableDef)
                logger.info("table {} created", table)
                connection.getExistingTableDefinition(table)
            } else {
                throw F2TException("Table $table is not existed and auto creation is not enabled, abort")
            }
        }
    }

    private fun prepare() {
        connection.prepareInsertion(TableDefinition(srcColumnDefinitions), table, tableDefinition)
    }

    private lateinit var batchDataCell: DataCell
    private fun getBatchCell(): DataCell {
        if (!this::batchDataCell.isInitialized) {
            batchDataCell = DataCell(batchNumber, srcColumnDefinitions.size - 1)
        }
        return batchDataCell
    }

    fun writeRow(row: DataRow) {
        val sortedRow = if (batchNumber < 0L) DataRow(
            row.rowNo,
            fromSrcColumnMapper.map { row.cells[it] }
        ) else {
            DataRow(
                row.rowNo,
                fromSrcColumnMapper.map { if (it < row.cells.size) row.cells[it] else getBatchCell() }
            )
        }
        connection.writeRow(table, sortedRow)
        rowCount++
    }

}
