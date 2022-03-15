/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.csv

import com.hagoapp.f2t.*
import com.hagoapp.f2t.datafile.*
import com.hagoapp.f2t.util.DateTimeTypeUtils
import com.hagoapp.f2t.util.JDBCTypeUtils
import com.hagoapp.util.EncodingUtils
import com.hagoapp.util.NumericUtils
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.mozilla.universalchardet.UnicodeBOMInputStream
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.sql.JDBCType
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import java.time.temporal.Temporal

class CSVDataReader : Reader {

    companion object {
        private val PREDEFINED_FORMAT_NAMES = listOf(
            "CSVFormat.DEFAULT",
            "CSVFormat.RFC4180",
            "CSVFormat.EXCEL",
            "CSVFormat.INFORMIX_UNLOAD",
            "CSVFormat.INFORMIX_UNLOAD_CSV",
            "CSVFormat.MYSQL",
            "CSVFormat.ORACLE",
            "CSVFormat.POSTGRESQL_CSV",
            "CSVFormat.POSTGRESQL_TEXT",
            "CSVFormat.TDF"
        )
    }

    private lateinit var fileInfo: FileInfoCsv
    private var loaded = false
    private lateinit var format: CSVFormat
    private var currentRow = 0
    private val data = mutableListOf<List<String>>()
    private lateinit var columns: Map<Int, FileColumnDefinition>
    private var rowCount = -1
    private val logger = F2TLogger.getLogger()
    private val columnDeterminerMap = mutableMapOf<String, DataTypeDeterminer>()
    private var defaultDeterminer: DataTypeDeterminer = LeastTypeDeterminer()
    private var skipTypeInfer = false
    private var predefinedFormats = listOf<CSVFormat>(
        CSVFormat.DEFAULT,
        CSVFormat.RFC4180,
        CSVFormat.EXCEL,
        CSVFormat.INFORMIX_UNLOAD,
        CSVFormat.INFORMIX_UNLOAD_CSV,
        CSVFormat.MYSQL,
        CSVFormat.ORACLE,
        CSVFormat.POSTGRESQL_CSV,
        CSVFormat.POSTGRESQL_TEXT,
        CSVFormat.TDF
    )
    private lateinit var formats: List<CSVFormat>

    override fun setupTypeDeterminer(determiner: DataTypeDeterminer): Reader {
        defaultDeterminer = determiner
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

    private fun getDeterminer(column: String): DataTypeDeterminer {
        return columnDeterminerMap[column] ?: defaultDeterminer
    }

    override fun open(fileInfo: FileInfo) {
        this.fileInfo = fileInfo as FileInfoCsv
        prepare(this.fileInfo)
        val charset = charsetForFile(fileInfo)
        for (i in formats.indices) {
            FileInputStream(fileInfo.filename!!).use { raw ->
                UnicodeBOMInputStream(raw, true).use { fi ->
                    try {
                        val fmt = formats[i]
                        parseCSV(fi, charset, fmt)
                        this.format = fmt
                        this.loaded = true
                        currentRow = 0
                        logger.debug("parsing csv: ${fileInfo.filename} using ${PREDEFINED_FORMAT_NAMES[i]} successfully")
                    } catch (ex: Exception) {
                        logger.error("parsing csv: ${fileInfo.filename} using ${PREDEFINED_FORMAT_NAMES[i]} failed: $ex, try next format")
                    }
                }
            }
            if (this.loaded) break
        }
        if (!this.loaded) {
            throw F2TException("File parsing for ${fileInfo.filename} failed")
        }
    }

    override fun getRowCount(): Int? {
        return if (rowCount < 0) null else rowCount
    }

    private fun charsetForFile(fileInfo: FileInfoCsv): Charset {
        val enc = when {
            fileInfo.encoding != null -> Charset.forName(fileInfo.encoding)
            fileInfo.filename == null -> StandardCharsets.UTF_8
            else -> Charset.forName(EncodingUtils.guessEncoding(fileInfo.filename!!))
        }
        logger.debug("file encoding is $enc")
        return enc
    }

    private fun checkLoad() {
        if (!loaded) {
            throw F2TException("file not opened")
        }
    }

    override fun findColumns(): List<FileColumnDefinition> {
        checkLoad()
        return columns.values.sortedBy { it.name }
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        checkLoad()
        return columns.values.toList().sortedBy { it.name }
    }

    override fun getSupportedFileType(): Set<Int> {
        return setOf(FileInfoCsv.FILE_TYPE_CSV)
    }

    override fun close() {
        data.clear()
    }

    override fun hasNext(): Boolean {
        return currentRow < data.size
    }

    override fun next(): DataRow {
        if (!hasNext()) {
            throw F2TException("No more line")
        }
        val row = DataRow(
            currentRow.toLong(),
            data[currentRow].mapIndexed { i, cell ->
                //logger.debug("convert '$cell' to ${columns.getValue(i).dataType!!}")
                DataCell(JDBCTypeUtils.toTypedValue(cell, columns.getValue(i).dataType!!), i)
            }
        )
        currentRow++
        return row
    }

    private fun prepare(fileInfo: FileInfoCsv) {
        this.formats = predefinedFormats.map { fmt ->
            CSVFormat.Builder.create(fmt).setHeader().setSkipHeaderRecord(true)
                .setDelimiter(fileInfo.delimiter).setQuote(fileInfo.quote).build()
        }
    }

    private fun parseCSV(ist: InputStream, charset: Charset, format: CSVFormat) {
        CSVParser.parse(ist, charset, format).use { parser ->
            columns = parser.headerMap.entries.associate { Pair(it.value, FileColumnDefinition(it.key, it.value)) }
            if (skipTypeInfer) {
                columns.values.forEach { it.possibleTypes = setOf(JDBCType.NCHAR, JDBCType.NVARCHAR, JDBCType.NCLOB) }
            }
            rowCount = 0
            parser.forEachIndexed { i, record ->
                if (record.size() != columns.size) {
                    throw F2TException("format error found in line $i of ${fileInfo.filename}")
                }
                val row = mutableListOf<String>()
                record.forEachIndexed { j, item ->
                    val cell = item.trim()
                    row.add(cell)
                    if (!skipTypeInfer) {
                        setupColumnDefinition(columns.getValue(j), cell)
                    }
                }
                data.add(row)
                rowCount++
            }
            columns.values.forEach { column ->
                //column.dataType = JDBCTypeUtils.guessMostAccurateType(column.possibleTypes.toList())
                column.dataType = getDeterminer(column.name).determineTypes(column.possibleTypes, column.typeModifier)
            }
            logger.debug("inferred: ${columns.values.associate { Pair(it.name, it.dataType) }}")
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

    private fun formatDateTime(value: Temporal?): String? {
        return when (value) {
            null -> null
            is ZonedDateTime -> value.format(DateTimeTypeUtils.getDefaultDateTimeFormatter())
            is LocalDate -> value.format(DateTimeTypeUtils.getDefaultDateFormatter())
            is LocalTime -> value.format(DateTimeTypeUtils.getDefaultTimeFormatter())
            else -> value.toString()
        }
    }
}
