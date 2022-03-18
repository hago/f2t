/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.DataCell
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.datafile.*
import com.hagoapp.f2t.util.DateTimeTypeUtils
import com.hagoapp.f2t.util.JDBCTypeUtils
import com.hagoapp.f2t.util.ParquetTypeUtils
import com.hagoapp.util.EncodingUtils
import com.hagoapp.util.NumericUtils
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetReader
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import java.time.temporal.Temporal

class ParquetFileReader : Reader {
    private lateinit var parquetFile: FileInfoParquet
    private lateinit var columns: List<FileColumnDefinition>
    private val columnDeterminerMap = mutableMapOf<String, DataTypeDeterminer>()
    private var defaultDeterminer: DataTypeDeterminer = LeastTypeDeterminer()
    private var skipTypeInfer = false
    private var currentLine = 0
    private var rowCount = -1
    private val data = mutableListOf<List<String>>()

    override fun open(fileInfo: FileInfo) {
        if (fileInfo !is FileInfoParquet) {
            throw java.lang.UnsupportedOperationException("not a parquet config")
        }
        parquetFile = fileInfo
        val path = Path(fileInfo.filename)
        ParquetReader.builder(AvroReadSupport<GenericData.Record>(GenericData.get()), path).build().use { reader ->
            while (true) {
                val record = reader.read()
                record ?: break
                if (rowCount < 0) {
                    parseHeader(record)
                    rowCount = 0
                } else {
                    parseRecord(record)
                    rowCount++
                }
            }
        }
        if (rowCount < 0) {
            throw UnsupportedOperationException("empty parquet")
        }
    }

    private fun parseRecord(record: GenericData.Record) {
        data.add(record.schema.fields.map { field ->
            if (!skipTypeInfer) {
                val col = columns.first { it.order == field.order().ordinal }
                val existingTypes = col.possibleTypes
                val possibleTypes = ParquetTypeUtils.guessJDBCType(field)
                col.possibleTypes = JDBCTypeUtils.combinePossibleTypes(existingTypes, possibleTypes)
                setupColumnDefinition(col, field.name())
            }
            field.name()
        })
    }

    private fun formatDateTime(value: Temporal?): String? {
        return when (value) {
            null -> null
            is ZonedDateTime -> value.format(DateTimeTypeUtils.getDefaultDateTimeFormatter())
            is LocalDate -> value.format(DateTimeTypeUtils.getDefaultDateFormatter())
            is LocalTime -> value.format(DateTimeTypeUtils.getDefaultTimeFormatter())
            else -> value.toString()
        }
    }

    private fun setupColumnDefinition(columnDefinition: FileColumnDefinition, cell: String) {
        val possibleTypes = if (cell.isNotBlank()) JDBCTypeUtils.guessTypes(cell).toSet() else emptySet()
        //logger.debug("value '{}' could be types: {}", cell, possibleTypes)
        val existTypes = columnDefinition.possibleTypes
        columnDefinition.possibleTypes = JDBCTypeUtils.combinePossibleTypes(existTypes, possibleTypes)
        //logger.debug("combined '{}'", columnDefinition.possibleTypes)
        val typeModifier = columnDefinition.typeModifier
        if (cell.length > typeModifier.maxLength) {
            typeModifier.maxLength = cell.length
        }
        val dt = formatDateTime(DateTimeTypeUtils.stringToTemporalOrNull(cell))
        if ((dt != null) && (dt.length > typeModifier.maxLength)) {
            typeModifier.maxLength = dt.length
        }
        val p = NumericUtils.detectPrecision(cell)
        if (p.first > typeModifier.precision) {
            typeModifier.precision = p.first
        }
        if (p.second > typeModifier.scale) {
            typeModifier.scale = p.second
        }
        setRange(columnDefinition, cell)
        if (!typeModifier.isContainsNonAscii && !EncodingUtils.isAsciiText(cell)) {
            typeModifier.isContainsNonAscii = true
        }
        if (!typeModifier.isNullable && cell.isEmpty()) {
            typeModifier.isNullable = true
        }
        if (!columnDefinition.isContainsEmpty && cell.isEmpty()) {
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

    private fun parseHeader(record: GenericData.Record) {
        val schema = record.schema
        columns = schema.fields.map { field ->
            val fileCol = FileColumnDefinition(field.name(), field.order().ordinal)
            fileCol.databaseTypeName = field.schema().type.name
            if (skipTypeInfer) {
                val type = ParquetTypeUtils.mapAvroTypeToJDBCType(field.schema().type)
                fileCol.possibleTypes = setOf(type)
                fileCol.dataType = type
            }
            fileCol
        }
    }

    override fun getRowCount(): Int {
        return rowCount
    }

    override fun findColumns(): List<FileColumnDefinition> {
        return columns
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        columns.forEach { col ->
            val determiner = columnDeterminerMap[col.name] ?: defaultDeterminer
            col.dataType = determiner.determineTypes(col.possibleTypes, col.typeModifier)
        }
        return columns
    }

    override fun getSupportedFileType(): Set<Int> {
        return setOf(FileInfoParquet.FILE_TYPE_PARQUET)
    }

    override fun setupTypeDeterminer(determiner: DataTypeDeterminer): Reader {
        this.defaultDeterminer = determiner
        return this
    }

    override fun setupColumnTypeDeterminer(column: String, determiner: DataTypeDeterminer): Reader {
        columnDeterminerMap[column] = determiner
        return this
    }

    override fun skipTypeInfer(): Reader {
        skipTypeInfer = true
        return this
    }

    override fun close() {
        data.clear()
    }

    override fun hasNext(): Boolean {
        return currentLine < rowCount
    }

    override fun next(): DataRow {
        val cells = data[currentLine].mapIndexed { index, cell ->
            val v = JDBCTypeUtils.toTypedValue(cell, columns[index].dataType)
            DataCell(v, index)
        }
        currentLine++
        return DataRow(currentLine.toLong(), cells)
    }
}
