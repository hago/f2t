/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.excel

import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.DataCell
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.datafile.*
import com.hagoapp.f2t.util.JDBCTypeUtils
import org.apache.poi.ss.usermodel.*
import java.io.FileInputStream
import java.sql.JDBCType
import java.time.ZoneId
import java.time.ZonedDateTime

class ExcelDataFileReader : Reader {
    private lateinit var infoExcel: FileInfoExcel
    private lateinit var workbook: Workbook
    private lateinit var sheet: Sheet
    private var currentRow = 0
    private lateinit var columns: Map<Int, FileColumnDefinition>

    override fun findColumns(): List<FileColumnDefinition> {
        if (!this::columns.isInitialized) {
            columns = sheet.getRow(sheet.firstRowNum).mapIndexed { i, cell ->
                Pair(i, FileColumnDefinition(i, cell.stringCellValue))
            }.toMap()
        }
        return columns.values.sortedBy { it.index }
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        if (!this::columns.isInitialized || columns.values.any { it.inferredType == null }) {
            val lastRowNum = if (sampleRowCount <= 0) sheet.lastRowNum else (sheet.firstRowNum + sampleRowCount).toInt()
            for (i in sheet.firstRowNum + 1..lastRowNum) {
                val row = sheet.getRow(i)
                if (row.lastCellNum > columns.size) {
                    throw F2TException("format error in ${infoExcel.filename}, line $i contains more cells than field row")
                }
                for (colIndex in columns.keys.indices) {
                    val cell = row.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)
                    val possibleTypes = guessCellType(cell)
                    val existingTypes = columns.getValue(colIndex).possibleTypes
                    columns.getValue(colIndex).possibleTypes = JDBCTypeUtils
                        .combinePossibleTypes(existingTypes.toList(), possibleTypes).toMutableSet()
                }
            }
            columns.values.forEach { it.inferredType = JDBCTypeUtils.guessMostAccurateType(it.possibleTypes.toList()) }
        }
        return columns.values.sortedBy { it.index }
    }

    override fun getSupportedFileType(): Set<Int> {
        return setOf(FileInfoExcel.FILE_TYPE_EXCEL, FileInfoExcelX.FILE_TYPE_EXCEL_OPEN_XML)
    }

    override fun close() {
        try {
            workbook.close()
        } catch (e: Throwable) {
            //
        }
    }

    override fun hasNext(): Boolean {
        return currentRow < (sheet.lastRowNum - sheet.firstRowNum)
    }

    override fun next(): DataRow {
        if (!hasNext()) {
            throw F2TException("No more line")
        }
        currentRow++
        val rawRow = sheet.getRow(currentRow + sheet.firstRowNum)
        val cells = mutableListOf<Cell>()
        for (colIndex in columns.keys.indices) {
            cells.add(rawRow.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK))
        }
        return DataRow(
            currentRow.toLong() - 1,
            cells.mapIndexed { i, cell ->
                DataCell(getCellValue(cell, columns.getValue(i).inferredType!!), i)
            }
        )
    }

    override fun open(fileInfo: FileInfo) {
        if (fileInfo !is FileInfoExcel) {
            throw F2TException("Not a FileInfoExcel class")
        }
        infoExcel = fileInfo
        workbook = WorkbookFactory.create(FileInputStream(fileInfo.filename))
        sheet = when {
            infoExcel.sheetIndex != null && workbook.getSheetAt(infoExcel.sheetIndex!!) != null ->
                workbook.getSheetAt(infoExcel.sheetIndex!!)
            infoExcel.sheetName != null && workbook.getSheet(infoExcel.sheetName) != null ->
                workbook.getSheet(infoExcel.sheetName)
            else -> workbook.getSheetAt(0)
        }
    }

    override fun getRowCount(): Int? {
        return if (!this::sheet.isInitialized) null else (sheet.lastRowNum - sheet.firstRowNum)
    }

    private fun guessCellType(cell: Cell): List<JDBCType> {
        return when {
            cell.cellType == CellType.BOOLEAN -> listOf(JDBCType.BOOLEAN)
            (cell.cellType == CellType.NUMERIC) && DateUtil.isCellDateFormatted(cell) -> listOf(JDBCType.TIMESTAMP)
            cell.cellType == CellType.NUMERIC -> {
                val nv = cell.numericCellValue
                if (nv == nv.toLong().toDouble()) listOf(JDBCType.DOUBLE, JDBCType.BIGINT) else listOf(
                    JDBCType.DOUBLE
                )
            }
            cell.cellType == CellType.BLANK -> listOf()
            else -> JDBCTypeUtils.guessTypes(cell.stringCellValue)
        }
    }

    private fun getCellValue(cell: Cell, type: JDBCType): Any? {
        return when {
            cell.cellType == CellType.BOOLEAN -> cell.booleanCellValue
            (cell.cellType == CellType.NUMERIC) && DateUtil.isCellDateFormatted(cell) -> ZonedDateTime.ofInstant(
                cell.dateCellValue.toInstant(),
                ZoneId.systemDefault()
            )
            cell.cellType == CellType.NUMERIC -> if (type == JDBCType.BIGINT) cell.numericCellValue.toLong() else cell.numericCellValue
            cell.cellType == CellType.BLANK -> cell.stringCellValue
            else -> JDBCTypeUtils.toTypedValue(cell.stringCellValue, type)
        }
    }
}
