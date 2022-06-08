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

class ParquetColumnarReader(parquetFileName: String) : Closeable {
    private val reader: ParquetReader<GenericData.Record>
    private val columns: List<ParquetColumn>

    data class ParquetColumn(
        val name: String,
        val typeName: String,
        val order: Int
    )

    init {
        val path = Path(parquetFileName)
        reader = ParquetReader.builder(AvroReadSupport<GenericData.Record>(GenericData.get()), path).build()
        val record = reader.read()
        val schema = record.schema
        columns = schema.fields.map { field ->
            ParquetColumn(field.name(), field.schema().type.name, field.order().ordinal)
        }
    }

    fun findColumns(): List<ParquetColumn> {
        return columns
    }

    fun readColumns(columnNames: List<String>, sizeToRead: Int = 0): Map<String, List<Any?>> {
        var count = 0
        val ret = columnNames.associateWith { mutableListOf<Any?>() }
        val missed = columnNames.filter { columns.none { colDef -> colDef.name == it } }
        if (missed.isNotEmpty()) {
            throw IllegalArgumentException("Invalid columns: ${missed.joinToString(", ")}")
        }
        val indices = columnNames.associateWith { columns.first { colDef -> colDef.name == it }.order }
        while ((sizeToRead <= 0) || (count < sizeToRead)) {
            val record = reader.read() ?: break
            columnNames.forEach { colName ->
                ret.getValue(colName).add(record[indices.getValue(colName)])
            }
            count++
        }
        return ret
    }

    override fun close() {
        try {
            reader.close()
        } catch (e: Throwable) {
            throw RuntimeException(String.format("Parquet columnar reader close error: %s", e.message), e)
        }
    }
}
