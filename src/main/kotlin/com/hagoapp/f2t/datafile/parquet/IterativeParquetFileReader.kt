/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.DataCell
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer
import com.hagoapp.f2t.datafile.FileInfo
import com.hagoapp.f2t.datafile.FileTypeDeterminer
import com.hagoapp.f2t.datafile.Reader
import com.hagoapp.f2t.util.ParquetTypeUtils
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetReader
import java.sql.JDBCType

class IterativeParquetFileReader : Reader {

    private lateinit var parquetFile: FileInfoParquet
    private lateinit var columns: List<FileColumnDefinition>
    private var defaultDeterminer = FileTypeDeterminer(FileColumnTypeDeterminer.LeastTypeDeterminer)
    private var skipTypeInfer = false
    private var lineData: DataRow? = null
    private var lineDataRead = false
    private var currentLine = 0L
    private lateinit var parquetReader: ParquetReader<GenericData.Record>

    override fun open(fileInfo: FileInfo) {
        if (fileInfo !is FileInfoParquet) {
            throw java.lang.UnsupportedOperationException("not a parquet config")
        }
        parquetFile = fileInfo
        val path = Path(fileInfo.filename)
        parquetReader = ParquetReader.builder(AvroReadSupport<GenericData.Record>(GenericData.get()), path).build()
        readHeader()
    }

    private fun readHeader() {
        val record = parquetReader.read()
        columns = record.schema.fields.mapIndexed { i, field ->
            val def = FileColumnDefinition(field.name(), i)
            if (!skipTypeInfer) {
                def.dataType = ParquetTypeUtils.mapAvroTypeToJDBCType(field.schema().type)
            } else {
                def.dataType = JDBCType.BLOB
            }
            def.possibleTypes = setOf(def.dataType)
            def
        }
        readRecord(record)
    }

    private fun readRecord(record: GenericData.Record?) {
        if (record == null) {
            lineData = null
            return
        }
        val cells = mutableListOf<DataCell>()
        for (i in 0 until record.schema.fields.size) {
            val cell = DataCell()
            cell.index = i
            cell.data = record[i]
            cells.add(cell)
        }
        lineData = DataRow(currentLine, cells)
        currentLine++;
    }

    override fun getRowCount(): Int? {
        throw UnsupportedOperationException("Not supported, row count can't be determined until all rows are read.")
    }

    override fun findColumns(): List<FileColumnDefinition> {
        return columns
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        // nothing to do, type inferring is done in open().
        return columns
    }

    override fun getSupportedFileType(): Set<Int> {
        // return empty set to avoid conflicts in Readers auto discovery
        return setOf()
    }

    override fun setupTypeDeterminer(determiner: FileTypeDeterminer): Reader {
        this.defaultDeterminer = determiner
        return this
    }

    override fun skipTypeInfer(): Reader {
        skipTypeInfer = true
        return this
    }

    override fun close() {
        try {
            parquetReader.close()
        } catch (e: Throwable) {
            throw RuntimeException("BufferedParquetFileReader ${this.parquetFile.filename} close error", e)
        }
    }

    override fun hasNext(): Boolean {
        return lineData != null
    }

    override fun next(): DataRow {
    }
}