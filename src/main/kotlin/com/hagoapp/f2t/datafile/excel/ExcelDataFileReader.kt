/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.excel

import com.hagoapp.f2t.ColumnTypeModifier
import com.hagoapp.f2t.DataCell
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.datafile.*
import com.hagoapp.f2t.util.DateTimeTypeUtils
import com.hagoapp.f2t.util.JDBCTypeUtils
import com.hagoapp.util.EncodingUtils
import com.hagoapp.util.NumericUtils
import org.apache.poi.ss.usermodel.*
import java.io.FileInputStream
import java.math.BigDecimal
import java.sql.JDBCType
import java.sql.JDBCType.*
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoField
import kotlin.math.max

class ExcelDataFileReader : Reader {
    private lateinit var infoExcel: FileInfoExcel
    private lateinit var workbook: Workbook
    private lateinit var sheet: Sheet
    private var currentRow = 0
    private lateinit var columns: Map<Int, FileColumnDefinition>
    private var determiner: DataTypeDeterminer? = null
    private val columnDeterminer = mutableMapOf<String, DataTypeDeterminer>()
    private var skipTypeInfer = false

    override fun findColumns(): List<FileColumnDefinition> {
        if (!this::columns.isInitialized) {
            columns = sheet.getRow(sheet.firstRowNum).mapIndexed { i, cell ->
                Pair(i, FileColumnDefinition(cellToString(cell), i))
            }.toMap()
        }
        return columns.values.sortedBy { it.name }
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        if (!this::columns.isInitialized || columns.values.any { it.dataType == null }) {
            if (skipTypeInfer) {
                columns.values.forEach { it.possibleTypes = setOf(NCHAR, NVARCHAR, NCLOB) }
            } else {
                val lastRowNum =
                    if (sampleRowCount <= 0) sheet.lastRowNum else (sheet.firstRowNum + sampleRowCount).toInt()
                for (i in sheet.firstRowNum + 1..lastRowNum) {
                    val row = sheet.getRow(i)
                    if (row.lastCellNum > columns.size) {
                        throw F2TException("format error in ${infoExcel.filename}, line $i contains more cells than field row")
                    }
                    for (colIndex in columns.keys.indices) {
                        val cell = row.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)
                        setupColumnDefinition(columns.getValue(colIndex), cell)
                    }
                }
            }
            columns.values.forEach { column ->
                column.dataType =
                    getTypeDeterminer(column.name).determineTypes(column.possibleTypes, column.typeModifier)
            }
        }
        return columns.values.sortedBy { it.name }
    }

    override fun getSupportedFileType(): Set<Int> {
        return setOf(FileInfoExcel.FILE_TYPE_EXCEL, FileInfoExcelX.FILE_TYPE_EXCEL_OPEN_XML)
    }

    override fun setupTypeDeterminer(determiner: DataTypeDeterminer): Reader {
        this.determiner = determiner
        return this
    }

    override fun setupColumnTypeDeterminer(column: String, determiner: DataTypeDeterminer): Reader {
        columnDeterminer[column] = determiner
        return this
    }

    override fun skipTypeInfer(): Reader {
        skipTypeInfer = true
        return this
    }

    private fun getTypeDeterminer(column: String): DataTypeDeterminer {
        return columnDeterminer[column] ?: determiner
        ?: throw java.lang.UnsupportedOperationException("No type determiner defined")
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
        val dataCells = columns.keys.map { colIndex ->
            val rawCell = rawRow.getCell(colIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK)
            val cellValue = getCellValue(rawCell, columns.getValue(colIndex).dataType)
            //println("${columns.getValue(colIndex).name}: $cellValue")
            DataCell(cellValue, colIndex)
        }
        return DataRow(currentRow.toLong() - 1, dataCells)
    }

    override fun open(fileInfo: FileInfo) {
        if (fileInfo !is FileInfoExcel) {
            throw F2TException("Not a FileInfoExcel class")
        }
        infoExcel = fileInfo
        workbook = WorkbookFactory.create(FileInputStream(fileInfo.filename!!))
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

    private fun setupColumnDefinition(columnDefinition: FileColumnDefinition, cell: Cell) {
        val possibleTypes = guessCellType(cell, columnDefinition.typeModifier)
        //println("guessed ${possibleTypes.size} types ${possibleTypes}")
        val existTypes = columnDefinition.possibleTypes
        columnDefinition.possibleTypes = JDBCTypeUtils.combinePossibleTypes(existTypes, possibleTypes)
        val typeModifier = columnDefinition.typeModifier
        if (columnDefinition.possibleTypes.contains(NCLOB) || columnDefinition.possibleTypes.contains(CLOB)) {
            if (cell.stringCellValue.length > typeModifier.maxLength) {
                typeModifier.maxLength = cell.stringCellValue.length
            }
            if (!typeModifier.isContainsNonAscii && !EncodingUtils.isAsciiText(cell.stringCellValue)) {
                typeModifier.isContainsNonAscii = true
            }
        }
        if (columnDefinition.possibleTypes.contains(DECIMAL) || columnDefinition.possibleTypes.contains(BIGINT)) {
            val p = NumericUtils.detectPrecision(cell.numericCellValue)
            if (p.first > typeModifier.precision) {
                typeModifier.precision = p.first
            }
            if (p.second > typeModifier.scale) {
                typeModifier.scale = p.second
            }
            val strValue = cell.numericCellValue.toString()
            if (strValue.length > typeModifier.maxLength) {
                typeModifier.maxLength = strValue.length
            }
        }
        if (columnDefinition.possibleTypes.contains(TIMESTAMP_WITH_TIMEZONE)) {
            val strValue = getDateCellValue(cell).format(DateTimeTypeUtils.getDefaultDateTimeFormatter())
            if (strValue.length > typeModifier.maxLength) {
                typeModifier.maxLength = strValue.length
            }
        } else if (columnDefinition.possibleTypes.contains(DATE)) {
            val strValue = getDateCellValue(cell).format(DateTimeTypeUtils.getDefaultDateFormatter())
            if (strValue.length > typeModifier.maxLength) {
                typeModifier.maxLength = strValue.length
            }
        } else if (columnDefinition.possibleTypes.contains(TIME)) {
            val strValue = getDateCellValue(cell).format(DateTimeTypeUtils.getDefaultTimeFormatter())
            if (strValue.length > typeModifier.maxLength) {
                typeModifier.maxLength = strValue.length
            }
        }
        setRange(columnDefinition, cellToString(cell))
        if (!columnDefinition.isContainsEmpty && cellToString(cell).isEmpty()) {
            columnDefinition.isContainsEmpty = true
        }
    }

    private fun setRange(columnDefinition: FileColumnDefinition, cell: String) {
        val num = cell.toBigDecimalOrNull()
        if (num != null) {
            if ((columnDefinition.maximum == null) || (columnDefinition.maximum < num)) {
                columnDefinition.maximum = num
            }
            if ((columnDefinition.minimum == null) || (columnDefinition.minimum > num)) {
                columnDefinition.minimum = num
            }
        }
    }

    private fun guessCellType(cell: Cell, modifier: ColumnTypeModifier): Set<JDBCType> {
        return when {
            cell.cellType == CellType.BOOLEAN -> setupBooleanType(cell, modifier)
            (cell.cellType == CellType.NUMERIC) && DateUtil.isCellDateFormatted(cell) ->
                guessDateTimeType(cell, modifier)
            cell.cellType == CellType.NUMERIC -> guessNumericType(cell, modifier)
            cell.cellType == CellType.BLANK -> setOf()
            else -> JDBCTypeUtils.guessTypes(cell.stringCellValue).toSet()
        }
    }

    private fun setupBooleanType(cell: Cell, typeModifier: ColumnTypeModifier): Set<JDBCType> {
        val len = max(true.toString().length, false.toString().length)
        if (len > typeModifier.maxLength) {
            typeModifier.maxLength = len
        }
        return setOf(BOOLEAN)
    }

    private fun guessDateTimeType(cell: Cell, typeModifier: ColumnTypeModifier): Set<JDBCType> {
        val ret = mutableSetOf<JDBCType>()
        val v = cell.localDateTimeCellValue
        if (v.isSupported(ChronoField.YEAR) && v.isSupported(ChronoField.HOUR_OF_DAY)) {
            ret.add(TIMESTAMP_WITH_TIMEZONE)
            val str = DateTimeTypeUtils.getDefaultDateTimeFormatter().format(v)
            if (str.length > typeModifier.maxLength) {
                typeModifier.maxLength = str.length
            }
        }
        if (v.isSupported(ChronoField.YEAR)) {
            ret.add(DATE)
            val str = DateTimeTypeUtils.getDefaultDateFormatter().format(v)
            if (str.length > typeModifier.maxLength) {
                typeModifier.maxLength = str.length
            }
        }
        if (v.isSupported(ChronoField.HOUR_OF_DAY)) {
            ret.add(TIME_WITH_TIMEZONE)
            val str = DateTimeTypeUtils.getDefaultTimeFormatter().format(v)
            if (str.length > typeModifier.maxLength) {
                typeModifier.maxLength = str.length
            }
        }
        return ret
    }

    private fun guessNumericType(cell: Cell, typeModifier: ColumnTypeModifier): Set<JDBCType> {
        val nv = cell.numericCellValue
        val ret = JDBCTypeUtils.guessFloatTypes(nv)
        if (ret.contains(DECIMAL) || ret.contains(BIGINT)) {
            val p = NumericUtils.detectPrecision(cell.numericCellValue)
            if (p.first > typeModifier.precision) {
                typeModifier.precision = p.first
            }
            if (p.second > typeModifier.scale) {
                typeModifier.scale = p.second
            }
            val strValue = cell.numericCellValue.toString()
            if (strValue.length > typeModifier.maxLength) {
                typeModifier.maxLength = strValue.length
            }
        }
        return ret
    }

    private val defaultFormatter = DataFormatter()
    private val cellToString = { cell: Cell ->
        defaultFormatter.formatCellValue(cell)
    }

    private fun getCellValue(cell: Cell, type: JDBCType): Any? {
        return when {
            cell.cellType == CellType.BOOLEAN -> cell.booleanCellValue
            (cell.cellType == CellType.NUMERIC) && DateUtil.isCellDateFormatted(cell) -> getDateCellValue(cell)
            cell.cellType == CellType.NUMERIC -> if (type == BIGINT) cell.numericCellValue.toLong()
            else JDBCTypeUtils.toTypedValue(cellToString(cell), type)
            cell.cellType == CellType.BLANK -> cellToString(cell)
            else -> JDBCTypeUtils.toTypedValue(cellToString(cell), type)
        }
    }

    private fun getDateCellValue(cell: Cell): ZonedDateTime {
        val v = cell.dateCellValue
        return ZonedDateTime.ofInstant(v.toInstant(), ZoneId.systemDefault())
    }
}
