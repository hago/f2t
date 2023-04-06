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
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.util.NoSuchElementException
import kotlin.jvm.Throws

/**
 * @author suncjs
 * @since 0.8
 */
class ParquetDataFileReader : Reader {

    private val logger = LoggerFactory.getLogger(ParquetDataFileReader::class.java)
    private lateinit var info: FileInfoParquet
    private lateinit var fis: FileInputStream
    private lateinit var reader: MemoryParquetReader
    private lateinit var columns: List<FileColumnDefinition>
    private var defaultDeterminer = FileTypeDeterminer(FileColumnTypeDeterminer.LeastTypeDeterminer)
    private var currentRow: Array<Any?>? = null
    private var rowNo = 0L

    @Throws(IOException::class)
    override fun open(fileInfo: FileInfo) {
        if (fileInfo !is FileInfoParquet) {
            throw UnsupportedOperationException("Not a parquet file config")
        }
        info = fileInfo
        val f = File(info.filename!!)
        if (!f.exists()) {
            throw FileNotFoundException("${info.filename} not found")
        }
        val size = f.length()
        fis = FileInputStream(f)
        reader = MemoryParquetReader.create(fis, size)
        val lines = reader.read(1)
        currentRow = if (lines.isNotEmpty()) lines[0] else null
    }

    override fun getRowCount(): Int? {
        return 0
    }

    override fun findColumns(): List<FileColumnDefinition> {
        if (!this::columns.isInitialized) {
            columns = reader.columns.mapIndexed { i, col ->
                val fCol = FileColumnDefinition(col.name, i)
                fCol.possibleTypes = setOf(col.dataType)
                fCol.dataType = col.dataType
                fCol
            }
        }
        return columns
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        return findColumns()
    }

    override fun getSupportedFileType(): Set<Int> {
        return setOf(FileInfoParquet.FILE_TYPE_PARQUET)
    }

    override fun setupTypeDeterminer(determiner: FileTypeDeterminer): Reader {
        defaultDeterminer = determiner
        return this
    }

    override fun skipTypeInfer(): Reader {
        return this
    }

    override fun close() {
        try {
            reader.close()
        } catch (e: Exception) {
            logger.error("close parquet reader error: {}", e.message)
        }
        try {
            fis.close()
        } catch (e: Exception) {
            logger.error("close parquet file error: {}", e.message)
        }
    }

    override fun hasNext(): Boolean {
        return currentRow != null
    }

    override fun next(): DataRow {
        val ret = currentRow ?: throw NoSuchElementException("End of iteration of parquet records")
        val lines = reader.read(1)
        if (lines.isNotEmpty()) {
            currentRow = lines[0]
            rowNo++
        } else {
            currentRow = null
        }
        val cells = ret.mapIndexed { index, cell ->
            DataCell(cell, index)
        }
        return DataRow(rowNo, cells)
    }
}
