/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.F2TLogger
import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.slf4j.Logger
import java.io.Closeable
import java.util.function.Function

/**
 * This class reads data from memory containing data in parquet file format.
 *
 * @author Chaojun Sun
 */
class MemoryParquetDataReader(input: ByteArray) : Closeable {

    companion object {
        private val logger: Logger = F2TLogger.getLogger()
        const val DEFAULT_FETCH_SIZE = 500000
    }

    private val row0: GenericData.Record
    val columns: List<String>
    private var rowNo = 0L
    private val reader: ParquetReader<GenericData.Record>
    private val columnSelections: IntArray

    init {
        val inputFile = MemoryInputFile(input)

        reader = AvroParquetReader.builder<GenericData.Record>(inputFile)
            .withCompatibility(true)
            .build()
        row0 = reader.read()
        columns = parseSchema(row0)
        columnSelections = IntArray(columns.size) { it }
    }

    private fun parseSchema(record: GenericData.Record): List<String> {
        val cols = record.schema.fields.map { field ->
            field.name()
        }
        return cols
    }

    fun withColumnSelectByNames(vararg columnNames: String) {
        columns.forEachIndexed { i, col ->
            val j = columnNames.indexOf(col)
            columnSelections[i] = if (j >= 0) i else -1
        }
    }

    fun withColumnSelectByIndexes(vararg columnIndexes: Int) {
        columnSelections.forEach { i -> columnSelections[i] = (if (columnIndexes.contains(i)) i else -1) }
    }

    fun withColumnNameSelector(selector: Function<String, Boolean>) {
        columns.forEachIndexed { i, col ->
            columnSelections[i] = if (selector.apply(col)) i else -1
        }
    }

    fun withColumnIndexSelector(selector: Function<Int, Boolean>) {
        columns.forEachIndexed { i, _ ->
            columnSelections[i] = if (selector.apply(i)) i else -1
        }
    }

    @JvmOverloads
    fun read(rowCount: Int? = null): Array<Array<Any?>> {
        val size = rowCount ?: DEFAULT_FETCH_SIZE
        val buffer = Array<Any?>(size * columns.size) { null }
        var rowPos = 0
        while (rowPos < size) {
            val row = readRow() ?: break
            val start = rowPos * columns.size
            for (i in columns.indices) {
                if (columnSelections[i] >= 0) {
                    buffer[start + i] = row[i]
                }
            }
            rowPos++
        }
        return Array(rowPos) { i ->
            Array(columns.size) { j ->
                buffer[i * columns.size + j]
            }
        }
    }

    private fun readRow(): GenericData.Record? {
        val row = if (rowNo == 0L) row0 else reader.read()
        rowNo++
        return row
    }

    fun skip(number: Long) {
        for (i in 0 until number) {
            readRow() ?: break
        }
    }

    override fun close() {
        try {
            reader.close()
        } catch (e: Throwable) {
            logger.error("Error: ${e.message}, while trying to close MemoryParquetDataReader $this")
        }
    }
}
