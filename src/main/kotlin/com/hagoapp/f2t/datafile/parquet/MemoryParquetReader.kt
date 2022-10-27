/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Closeable

/**
 * A parquet file reader which is capable to handle memory content loaded from parquet file.
 *
 * @author Chaojun Sun
 * @since 0.7
 */
@Deprecated(
    "This class was created in hurry and not a good implementation",
    replaceWith = ReplaceWith("MemoryParquetDataReader"),
    level = DeprecationLevel.WARNING
)
class MemoryParquetReader(input: ByteArray) : Closeable {

    companion object {

        private val logger: Logger = LoggerFactory.getLogger(MemoryParquetReader::class.java)

    }

    private val row0: List<RecordCell>
    val columns: List<String>
    private var rowNo = 0L
    private val reader: ParquetReader<GenericData.Record>

    init {
        val inputFile = MemoryInputFile(input)

        reader = AvroParquetReader.builder<GenericData.Record>(inputFile)
            .withCompatibility(true)
            .build()
        val record = reader.read()
        val info = parseSchema(record)
        columns = info.first
        row0 = info.second.mapIndexed { i, value -> RecordCell(columns[i], value) }
    }

    private fun parseSchema(record: GenericData.Record): Pair<List<String>, List<Any?>> {
        val cols = mutableListOf<String>()
        val row = record.schema.fields.mapIndexed { i, field ->
            //val type = field.schema().type
            cols.add(field.name())
            record[i]
        }
        return Pair(cols, row)
    }

    private fun parseRecord(record: GenericData.Record): List<RecordCell> {
        return List(record.schema.fields.size) { i ->
            RecordCell(record.schema.fields[i].name(), record[i])
        }
    }

    private fun filterColumns(row: List<RecordCell>, columnSelector: (String) -> Boolean): List<RecordCell> {
        return row.filter { p -> columnSelector(p.fieldName) }
    }

    @JvmOverloads
    fun readRow(rowCount: Int? = null, columnSelector: (String) -> Boolean = { _ -> true }): List<List<RecordCell>> {
        val ret = mutableListOf<List<RecordCell>>()
        var rowCountNeeded = rowCount ?: 1
        if (rowNo == 0L) {
            ret.add(filterColumns(row0, columnSelector))
            rowCountNeeded--
            rowNo += 1
        }
        for (i in 0 until rowCountNeeded) {
            val record = reader.read() ?: break
            ret.add(filterColumns(parseRecord(record), columnSelector))
            rowNo++
        }
        return ret
    }

    override fun close() {
        try {
            reader.close()
        } catch (e: Throwable) {
            //
        }
    }
}
