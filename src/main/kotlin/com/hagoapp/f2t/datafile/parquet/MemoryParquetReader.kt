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
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.InputStream
import java.lang.Exception
import java.lang.reflect.Method
import java.sql.JDBCType
import java.util.function.BiConsumer
import java.util.function.Predicate
import kotlin.jvm.Throws

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

        @JvmStatic
        @Throws(UnsupportedOperationException::class)
        fun mapParquetTypeToJdbcType(parquetType: Type): JDBCType {
            val primitiveType = parquetType.asPrimitiveType()
            when (primitiveType.primitiveTypeName) {
                PrimitiveTypeName.INT32 -> return JDBCType.INTEGER
                PrimitiveTypeName.INT64 -> return JDBCType.BIGINT
                PrimitiveTypeName.INT96 -> return JDBCType.NUMERIC
                PrimitiveTypeName.BOOLEAN -> return JDBCType.BOOLEAN
                PrimitiveTypeName.DOUBLE -> return JDBCType.DOUBLE
                PrimitiveTypeName.FLOAT -> return JDBCType.FLOAT
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> return JDBCType.BINARY
                PrimitiveTypeName.BINARY -> {
                    return when (parquetType.logicalTypeAnnotation) {
                        is StringLogicalTypeAnnotation -> JDBCType.CLOB
                        is DateLogicalTypeAnnotation -> JDBCType.DATE
                        is TimeLogicalTypeAnnotation -> JDBCType.TIME_WITH_TIMEZONE
                        is TimestampLogicalTypeAnnotation -> JDBCType.TIME_WITH_TIMEZONE
                        else -> {
                            logger.warn(
                                "Logical annotation {} of type {} is ignored as CLOB",
                                parquetType.logicalTypeAnnotation, parquetType
                            )
                            JDBCType.CLOB
                        }
                    }
                }

                else -> {
                    logger.warn("type {} is ignored as CLOB", parquetType)
                    return JDBCType.CLOB
                }
            }
        }
    }

    private val reader: ParquetFileReader
    private val schema: MessageType
    val columns: List<ColumnDefinition>
    private val columnsSelecting: Array<Boolean>
    private val columnValueMethods: Array<Method>
    private var currentRowGroup: PageReadStore
    private val ioFactory = ColumnIOFactory()
    private var requestedSchema: MessageType
    private lateinit var groupReader: RecordReader<Group>
    private lateinit var columnIO: MessageColumnIO
    private lateinit var groupRecordConverter: GroupRecordConverter
    private var rowsReadInGroup = 0L

    init {
        val opt = ParquetReadOptions.builder().build()
        reader = ParquetFileReader(input, opt)
        schema = reader.fileMetaData.schema
        columns = schema.fields.map { fieldToColumn(it) }
        columnValueMethods = schema.fields.map { t ->
            val methodName = t.asPrimitiveType().primitiveTypeName.getMethod
            Group::class.java.getMethod(methodName, Int::class.java, Int::class.java)
        }.toTypedArray()
        columnsSelecting = Array(columns.size) { true }
        currentRowGroup = reader.readNextRowGroup()
        requestedSchema = schema
        updateRequestedSchema()
    }

    private fun fieldToColumn(type: Type): ParquetColumnDefinition {
        val def = ParquetColumnDefinition()
        def.name = type.name
        def.parquetType = type.asPrimitiveType()
        def.dataType = mapParquetTypeToJdbcType(type)
        return def
    }

    fun fetchColumnByNames(vararg names: String): MemoryParquetReader {
        if (names.isNotEmpty()) {
            columnsSelecting.forEachIndexed { i, _ ->
                columnsSelecting[i] = names.any { it == columns[i].name }
            }
        } else {
            logger.warn("null or empty column names provided")
        }
        return this
    }

    fun fetchColumnByNameSelector(selector: Predicate<String>): MemoryParquetReader {
        columnsSelecting.forEachIndexed { i, _ ->
            columnsSelecting[i] = selector.test(columns[i].name)
        }
        if (columnsSelecting.none { it }) {
            logger.warn("No columns selected by name selector")
        }
        return this
    }

    fun fetchColumnByIndexes(vararg indexes: Int): MemoryParquetReader {
        if (indexes.isNotEmpty()) {
            columnsSelecting.forEachIndexed { i, _ ->
                columnsSelecting[i] = indexes.contains(i)
            }
        } else {
            logger.warn("null or empty column indexes provided")
        }
        return this
    }

    fun fetchColumnByIndexSelector(selector: Predicate<Int>): MemoryParquetReader {
        columnsSelecting.forEachIndexed { i, _ ->
            columnsSelecting[i] = selector.test(i)
        }
        if (columnsSelecting.none { it }) {
            logger.warn("No columns selected by index selector")
        }
        return this
    }

    fun skip(rowCount: Int): Int {
        return internalRead(rowCount)
    }

    private fun internalRead(rowCount: Int, rowProcessor: BiConsumer<Group, Int>? = null): Int {
        var rowsFetched = 0
        while (rowsFetched < rowCount) {
            logger.trace(
                "row read: {}, row No in group: {}, group count: {}",
                rowsFetched,
                rowsReadInGroup,
                currentRowGroup.rowCount
            )
            if (rowsReadInGroup >= currentRowGroup.rowCount) {
                currentRowGroup = reader.readNextRowGroup() ?: break
                buildGroupReader()
                continue
            }
            val group = groupReader.read()
            rowsFetched++
            rowsReadInGroup++
            rowProcessor?.accept(group, rowsFetched)
        }
        return rowsFetched
    }

    fun read(rowCount: Int): Array<Array<Any?>> {
        val buffer = Array<Any?>(rowCount * columns.size) { null }
        val consumer: BiConsumer<Group, Int> = BiConsumer<Group, Int> { group, rowNo ->
            for (i in columns.indices) {
                buffer[(rowNo - 1) * columns.size + i] = if (!columnsSelecting[i]) null
                else columnValueMethods[i].invoke(group, i, 0)
            }
        }
        val actualRead = internalRead(rowCount, consumer)
        val ret = Array(actualRead) {
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

    private fun buildGroupReader() {
        groupReader = columnIO.getRecordReader(currentRowGroup, groupRecordConverter)
        rowsReadInGroup = 0L
    }

    override fun close() {
        try {
            reader.close()
        } catch (e: Exception) {
            //
        }
    }
}
