/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetReader
import java.io.Closeable

/**
 * This class reads data from parquet files by specified columns and specified row count.
 *
 * @author Chaojun Sun
 * @since 0.6.2
 */
class ParquetColumnarReader(private val parquetFileName: String) : Closeable {
    private var reader: ParquetReader<GenericData.Record>? = null
    private val path: Path = Path(parquetFileName)
    private val columns: List<ParquetColumn>
    private var row0: List<Any?>
    private var currentRowNo = 0

    data class ParquetColumn(
        val name: String,
        val typeName: String,
        val order: Int
    )

    init {
        createReader()
        val record = reader!!.read()
        val schema = record.schema
        columns = schema.fields.mapIndexed { i, field ->
            ParquetColumn(field.name(), field.schema().type.name, i)
        }.sortedBy { it.order }
        row0 = readRow(record)
    }

    private fun createReader() {
        reader?.close()
        reader = null
        reader = ParquetReader.builder(AvroReadSupport<GenericData.Record>(GenericData.get()), path).build()
    }

    private fun readRow(record: GenericData.Record, filter: (String) -> Boolean = { _ -> true }): List<Any?> {
        val ret = mutableListOf<Any?>()
        for (i in record.schema.fields.indices) {
            if (!filter(record.schema.fields[i].name())) {
                continue
            }
            ret.add(record[i])
        }
        return ret
    }

    fun findColumns(): List<ParquetColumn> {
        return columns
    }

    /**
     * Reads specified count for specified columns.
     *
     * @param columnNames   specified columns to read
     * @param sizeToRead    specified row count to read
     * @return  a dictionary that contains each column as key and their value lists as value
     */
    @JvmOverloads
    fun readColumns(columnNames: List<String>? = null, sizeToRead: Int = 0): Map<String, List<Any?>> {
        val columnsRequired = columnNames ?: columns.map { it.name }
        val missed = columnsRequired.filter { columns.none { colDef -> colDef.name == it } }
        if (missed.isNotEmpty()) {
            throw IllegalArgumentException("Invalid columns: ${missed.joinToString(", ")}")
        }
        val indices = columnsRequired.associateWith { columns.first { colDef -> colDef.name == it }.order }
        var count = 0
        val ret = columnsRequired.associateWith { mutableListOf<Any?>() }
        while ((sizeToRead <= 0) || (count < sizeToRead)) {
            if (currentRowNo == 0) {
                columnsRequired.forEach { colName ->
                    ret.getValue(colName).add(row0[indices.getValue(colName)])
                }
            } else {
                val record = reader!!.read() ?: break
                val row = readRow(record) { colName -> columnsRequired.contains(colName) }
                columnsRequired.forEachIndexed { i, colName -> ret.getValue(colName).add(row[i]) }
            }
            count++
            currentRowNo++
        }
        return ret
    }

    fun reset() {
        createReader()
        currentRowNo = 0
        reader!!.read()
    }

    override fun close() {
        try {
            reader?.close()
            reader = null
        } catch (e: Throwable) {
            throw RuntimeException(String.format("Parquet columnar reader close error: %s", e.message), e)
        }
    }
}
