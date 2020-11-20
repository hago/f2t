/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.util.JDBCTypeUtils
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.Charset
import java.sql.JDBCType

class CSVDataReader : Reader {

    private lateinit var fileInfo: FileInfoCsv
    private var loaded = false
    private lateinit var format: CSVFormat
    private var currentRow = 0
    private var guessedTypes: Map<Int, Pair<String, List<JDBCType>>>? = null
    private val data = mutableListOf<DataRow>()
    private lateinit var columns: List<ColumnDefinition>
    private lateinit var parser: CSVParser

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
        val charset = Charset.forName(this.fileInfo.encoding)
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

    override fun findColumns(): List<ColumnDefinition> {
        if (this::columns.isInitialized) {
            columns = parser.headerMap.entries.map {
                ColumnDefinition(it.value, it.key)
            }
        }
        return columns
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<ColumnDefinition> {

    }

    override fun close() {
        try {
            if (this::parser.isInitialized) {
                parser.close()
            }
        } catch (e: Throwable) {
            //
        }
    }

    override fun hasNext(): Boolean {
        return currentRow < data.size
    }

    override fun next(): DataRow {
        if (!hasNext()) {
            throw F2TException("No more line")
        }
        val line = data[currentRow]
        currentRow++
        return line
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
        parser = CSVParser.parse(ist, charset, format)
        CSVParser.parse(ist, charset, format).use { parser ->
            val colCount = parser.headerMap.size
            val typeMap: MutableMap<Int, Pair<String, List<JDBCType>>> = mutableMapOf()
            parser.headerMap.forEach { (name, i) ->
                typeMap[i] = Pair(name, listOf())
            }
            columns =
                parser.forEachIndexed { i, csvRecord ->
                    if (csvRecord.size() != colCount) {
                        throw Exception("format error found in ${fileInfo.filename}")
                    }
                    csvRecord.forEachIndexed { j, cell ->
                        val possibleTypes = JDBCTypeUtils.guessTypes(cell.trim())
                        val existTypes = typeMap[j]!!.second
                        typeMap[j] =
                            Pair(typeMap[j]!!.first, JDBCTypeUtils.combinePossibleTypes(existTypes, possibleTypes))
                    }
                    data.add(csvRecord.map { cell -> cell.trim() })
                }
            guessedTypes = typeMap
            //logger.debug("Data type detected as: $typeMap")
        }
    }
}
