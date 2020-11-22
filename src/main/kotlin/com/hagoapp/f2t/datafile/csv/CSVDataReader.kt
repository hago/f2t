/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.csv

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.datafile.*
import com.hagoapp.util.EncodingUtils
import com.hagoapp.f2t.util.JDBCTypeUtils
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.Charset

class CSVDataReader : Reader {

    private lateinit var fileInfo: FileInfoCsv
    private var loaded = false
    private lateinit var format: CSVFormat
    private var currentRow = 0
    private val data = mutableListOf<List<String>>()
    private lateinit var columns: Map<Int, ColumnDefinition>

    private var formats: List<CSVFormat> = listOf<CSVFormat>(
        CSVFormat.DEFAULT, CSVFormat.RFC4180, CSVFormat.EXCEL, CSVFormat.INFORMIX_UNLOAD, CSVFormat.INFORMIX_UNLOAD_CSV,
        CSVFormat.MYSQL, CSVFormat.ORACLE, CSVFormat.POSTGRESQL_CSV, CSVFormat.POSTGRESQL_TEXT, CSVFormat.TDF
    )
    private val formatNames: List<String> = listOf(
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

    override fun open(fileInfo: FileInfo) {
        this.fileInfo = fileInfo as FileInfoCsv
        prepare(this.fileInfo)
        val charset = charsetForFile(fileInfo)
        for (i in formats.indices) {
            FileInputStream(fileInfo.filename).use { fi ->
                try {
                    val fmt = formats[i]
                    parseCSV(fi, charset, fmt)
                    this.format = fmt
                    this.loaded = true
                    currentRow = 0
                } catch (ex: Exception) {
                    //
                }
            }
            if (this.loaded) break
        }
        if (!this.loaded) {
            throw F2TException("File parsing for ${fileInfo.filename} failed")
        }
    }

    private fun charsetForFile(fileInfo: FileInfoCsv): Charset {
        return Charset.forName(fileInfo.encoding ?: EncodingUtils.guessEncoding(fileInfo.filename))
    }

    private fun checkLoad() {
        if (!loaded) {
            throw F2TException("file not opened")
        }
    }

    override fun findColumns(): List<ColumnDefinition> {
        checkLoad()
        return columns.values.sortedBy { it.index }
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<ColumnDefinition> {
        checkLoad()
        return columns.values.toList().sortedBy { it.index }
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
                DataCell(JDBCTypeUtils.toTypedValue(cell, columns.getValue(i).inferredType!!), i)
            }
        )
        currentRow++
        return row
    }

    override fun remove() {
        throw UnsupportedOperationException("Remove from csv reader is not supported")
    }

    private fun prepare(fileInfo: FileInfoCsv) {
        val customizeCSVFormat = { fmt: CSVFormat ->
            fmt.withFirstRecordAsHeader().withDelimiter(fileInfo.delimiter)
                //logger.debug("set limiter to ${extra.delimiter} for csv parser")
                .withQuote((fileInfo.quote))
        }
        this.formats = this.formats.map { fmt ->
            customizeCSVFormat(fmt)
        }
    }

    private fun parseCSV(ist: InputStream, charset: Charset, format: CSVFormat) {
        CSVParser.parse(ist, charset, format).use { parser ->
            columns = parser.headerMap.entries.map {
                Pair(it.value, ColumnDefinition(it.value, it.key))
            }.toMap()
            parser.forEachIndexed { i, record ->
                if (record.size() != columns.size) {
                    throw F2TException("format error found in line $i of ${fileInfo.filename}")
                }
                val row = mutableListOf<String>()
                record.forEachIndexed { j, item ->
                    val cell = item.trim()
                    row.add(cell)
                    val possibleTypes = JDBCTypeUtils.guessTypes(cell)
                    val existTypes = columns.getValue(j).possibleTypes
                    columns.getValue(j).possibleTypes =
                        JDBCTypeUtils.combinePossibleTypes(existTypes.toList(), possibleTypes).toMutableSet()
                }
                data.add(row)
            }
            columns.values.forEach { column ->
                column.inferredType = JDBCTypeUtils.guessMostAccurateType(column.possibleTypes.toList())
            }
        }
    }
}
