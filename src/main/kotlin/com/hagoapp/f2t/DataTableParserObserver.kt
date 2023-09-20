package com.hagoapp.f2t

import com.hagoapp.f2t.datafile.FileInfo
import com.hagoapp.f2t.datafile.ParseResult

/**
 * The observer implementation for a <code>DataTable</code> source.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
class DataTableParserObserver : ParseObserver {

    private lateinit var dataTable: DataTable<FileColumnDefinition>
    private lateinit var columnDefinitions: List<FileColumnDefinition>
    private val rows = mutableListOf<DataRow>()

    /**
     * Errors dueing processing, set during processing and can;t be modified from outside.
     */
    var errors = mutableListOf<Throwable>()
        private set

    /**
     * Errors during row reading, set during processing and can;t be modified from outside.
     */
    var rowErrors = mutableListOf<Pair<Int, Throwable>>()
        private set

    /**
     * Whether successful of process being observed.
     */
    val succeeded: Boolean
        get() = errors.isEmpty() && rowErrors.isEmpty()
    private var lineNo = 0
    private var completed: Boolean = false

    override fun onColumnTypeDetermined(columnDefinitionList: MutableList<FileColumnDefinition>) {
        columnDefinitions = columnDefinitionList
    }

    override fun onRowRead(row: DataRow) {
        rows.add(row)
        lineNo++
    }

    override fun onParseComplete(fileInfo: FileInfo, result: ParseResult) {
        completed = true
    }

    override fun onRowError(e: Throwable): Boolean {
        rowErrors.add(Pair(lineNo, e))
        lineNo++
        return true
    }

    override fun onError(e: Throwable) {
        errors.add(e)
    }

    /**
     * Find the data table object from process.
     *
     * @return data table
     */
    fun getDataTable(): DataTable<FileColumnDefinition> {
        if (!completed) {
            throw F2TException("parsing not completed")
        }
        if (!this::dataTable.isInitialized) {
            dataTable = DataTable(columnDefinitions, rows)
        }
        return dataTable
    }
}