package com.hagoapp.f2t

import com.hagoapp.f2t.datafile.FileInfo
import com.hagoapp.f2t.datafile.ParseResult

class DataTableParserObserver : ParseObserver {

    private lateinit var dataTable: DataTable
    private lateinit var columnDefinitions: List<ColumnDefinition>
    private val rows = mutableListOf<DataRow>()
    var errors = mutableListOf<Throwable>()
        private set
    var rowErrors = mutableListOf<Pair<Int, Throwable>>()
        private set

    val succeeded: Boolean
        get() = errors.isEmpty() && rowErrors.isEmpty()
    private var lineNo = 0
    private var completed: Boolean = false

    override fun onColumnTypeDetermined(columnDefinitionList: MutableList<ColumnDefinition>) {
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

    fun getDataTable(): DataTable {
        if (!completed) {
            throw F2TException("parsing not completed")
        }
        if (!this::dataTable.isInitialized) {
            dataTable = DataTable(columnDefinitions, rows)
        }
        return dataTable
    }
}