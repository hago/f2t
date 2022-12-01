/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.ColumnDefinition
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.io.RecordReader
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Type
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.InputStream
import java.lang.Exception
import java.sql.JDBCType
import java.util.function.Predicate

/**
 * A parquet reader to read data from byte array and stream.
 *
 * @since 0.7.5
 * @author suncjs
 */
class MemoryParquetReader(input: MemoryInputFile) : Closeable {

    companion object {

        private val logger = LoggerFactory.getLogger(MemoryParquetReader::class.java)

        @JvmStatic
        fun create(buffer: ByteArray): MemoryParquetReader {
            val file = MemoryInputFile(buffer)
            return MemoryParquetReader(file)
        }

        @JvmStatic
        fun create(stream: InputStream): MemoryParquetReader {
            val size = stream.available()
            logger.warn("size of stream not specified, {} estimated", size)
            return create(stream, size.toLong())
        }

        @JvmStatic
        fun create(stream: InputStream, length: Long): MemoryParquetReader {
            val file = MemoryInputFile(stream, length)
            return MemoryParquetReader(file)
        }
    }

    private val reader: ParquetFileReader
    private val schema: MessageType
    val columns: List<ColumnDefinition>
    private val columnsSelecting: Array<Boolean>
    private var currentRowGroup: PageReadStore
    private val ioFactory = ColumnIOFactory()
    private var requestedSchema: MessageType
    private lateinit var groupReader: RecordReader<Group>
    private lateinit var columnIO: MessageColumnIO
    private lateinit var groupRecordConverter: GroupRecordConverter

    init {
        val opt = ParquetReadOptions.builder().build()
        reader = ParquetFileReader(input, opt)
        schema = reader.fileMetaData.schema
        columns = schema.fields.map { fieldToColumn(it) }
        columnsSelecting = Array(columns.size) { true }
        requestedSchema = schema
        updateRequestedSchema()
        currentRowGroup = reader.readNextRowGroup()
    }

    private fun fieldToColumn(type: Type): ParquetColumnDefinition {
        val def = ParquetColumnDefinition()
        def.name = type.name
        def.parquetType = type.asPrimitiveType()
        def.dataType = JDBCType.CLOB
        return def
    }

    fun fetchColumnByNames(vararg names: String): MemoryParquetReader {
        columnsSelecting.forEachIndexed { i, _ ->
            columnsSelecting[i] = names.any { it == columns[i].name }
        }
        return this
    }

    fun fetchColumnByNameSelector(selector: Predicate<String>): MemoryParquetReader {
        columnsSelecting.forEachIndexed { i, _ ->
            columnsSelecting[i] = selector.test(columns[i].name)
        }
        return this
    }

    fun fetchColumnByIndexes(vararg indexes: Int): MemoryParquetReader {
        columnsSelecting.forEachIndexed { i, _ ->
            columnsSelecting[i] = indexes.contains(i)
        }
        return this
    }

    fun fetchColumnByIndexSelector(selector: Predicate<Int>): MemoryParquetReader {
        columnsSelecting.forEachIndexed { i, _ ->
            columnsSelecting[i] = selector.test(i)
        }
        return this
    }

    fun skip(rowCount: Int): Int {
        var rowsSkipped = 0
        while (rowsSkipped < rowCount) {
            val group = groupReader.read()
            if (group == null) {
                currentRowGroup = reader.readNextRowGroup() ?: break
                buildGroupReader()
                continue
            }
            rowsSkipped++
        }
        return rowsSkipped
    }

    fun read(rowCount: Int): Array<Array<Any?>> {
        val buffer = Array<Any?>(rowCount * columns.size) { null }
        var rowsRead = 0
        while (rowsRead < rowCount) {
            val group = groupReader.read()
            if (group == null) {
                currentRowGroup = reader.readNextRowGroup() ?: break
                buildGroupReader()
                continue
            }
            for (i in columns.indices) {
                buffer[rowsRead * columns.size + i] = if (columnsSelecting[i]) group.getString(i, 0) else null
            }
            rowsRead++
        }
        val ret = Array(rowsRead) {
            Array<Any?>(columns.size) { null }
        }
        for (i in ret.indices) {
            System.arraycopy(buffer, i * columns.size, ret[i], 0, columns.size)
        }
        return ret
    }

    private fun updateRequestedSchema() {
        columnIO = ioFactory.getColumnIO(requestedSchema)
        groupRecordConverter = GroupRecordConverter(requestedSchema)
        buildGroupReader()
    }

    private fun buildGroupReader(): RecordReader<Group> {
        return columnIO.getRecordReader(currentRowGroup, groupRecordConverter)
    }

    override fun close() {
        try {
            reader.close()
        } catch (e: Exception) {
            //
        }
    }
}
