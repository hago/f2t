/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.DataTable
import com.hagoapp.f2t.util.ParquetTypeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.*
import org.slf4j.LoggerFactory
import java.sql.Time
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZonedDateTime
import java.util.*

/**
 * A writer for parquet file.
 *
 * @property data  data table containing schema definition and rows to write
 * @property config    configuration file for target parquet file
 */
class ParquetWriter(private val data: DataTable<out ColumnDefinition>, private val config: ParquetWriterConfig) {

    private val schemaValue: String
    private val schema: MessageType = MessageType(config.name, data.columnDefinition.map { col ->
        val pn = ParquetTypeUtils.mapJdbcTypeToPrimitiveTypeName(col.dataType)
        val lta = ParquetTypeUtils.mapJdbcTypeToLogicalAnnotation(col.dataType)
        val p = if (lta != null) Types.required(pn).`as`(lta).named(col.name) else Types.required(pn).named(col.name)
        p
    })
    private val logger = LoggerFactory.getLogger(ParquetWriter::class.java)

    init {
        schemaValue = schema.toString()
    }

    /**
     * Create target parquet file, write data and close.
     */
    fun write() {
        val cols = data.columnDefinition.map { it.name }
        val f = LocalOutputFile(config.parquetFileName)
        val conf = Configuration()
        ExampleParquetWriter.builder(f).withConf(conf).withType(schema).build().use { writer ->
            logger.debug("rows: {}", data.rows)
            data.rows.forEach { row ->
                logger.debug("row {}: {}", row.rowNo, row.cells)
                val group = SimpleGroup(schema)
                row.cells.forEach { cell ->
                    val i = cell.index
                    setDataForGroup(group, cols[i], cell.data, schema.fields[i])
                }
                writer.write(group)
            }
        }
    }

    private fun setDataForGroup(group: SimpleGroup, colName: String, data: Any?, type: Type) {
        if (data == null) {
            logger.debug("skip {} for null", colName)
            return
        }
        logger.debug("set data {} as {}", data, data::class.java)
        when (type.asPrimitiveType().primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.INT32 -> group.add(
                colName,
                ParquetTypeUtils.convertJavaDataForParquetInt(data)
            )

            PrimitiveType.PrimitiveTypeName.INT64 -> group.add(
                colName,
                ParquetTypeUtils.convertJavaDataForParquetLong(data)
            )

            PrimitiveType.PrimitiveTypeName.BOOLEAN -> group.add(colName, data as Boolean)
            PrimitiveType.PrimitiveTypeName.FLOAT -> group.add(colName, data as Float)
            PrimitiveType.PrimitiveTypeName.DOUBLE -> group.add(colName, data as Double)
            PrimitiveType.PrimitiveTypeName.BINARY -> setBinaryValueForGroup(group, colName, data, type)
            else -> {
                logger.error("Unknown primitiveTypeName {}", type.asPrimitiveType().primitiveTypeName)
                throw UnsupportedOperationException("Unknown primitiveTypeName ${type.asPrimitiveType().primitiveTypeName}")
            }
        }
    }

    private fun setBinaryValueForGroup(group: SimpleGroup, colName: String, data: Any, type: Type) {
        val p = type.asPrimitiveType()
        logger.debug("set binary data {} as {}", type.logicalTypeAnnotation, p.logicalTypeAnnotation)
        when (p.logicalTypeAnnotation) {
            null -> group.add(colName, ParquetTypeUtils.convertJavaDataForParquetBinary(data))
            is LogicalTypeAnnotation.StringLogicalTypeAnnotation -> group.add(colName, data as String)
            is LogicalTypeAnnotation.TimeLogicalTypeAnnotation -> {
                when (data) {
                    is LocalTime -> group.add(colName, ParquetTypeUtils.convertLocalTimeForParquet(data))
                    is Time -> group.add(colName, ParquetTypeUtils.convertTimeForParquet(data))
                    else -> {
                        logger.error("data '{}' should time, however is {}", data, data::class.java)
                        throw UnsupportedOperationException("data '$data' should time, however is ${data::class.java}")
                    }
                }
            }

            is LogicalTypeAnnotation.DateLogicalTypeAnnotation -> {
                val d = data as LocalDate
                group.add(colName, ParquetTypeUtils.convertLocalDateForParquet(d))
            }

            is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation -> {
                when (data) {
                    is ZonedDateTime -> group.add(colName, ParquetTypeUtils.convertZonedDateTimeForParquet(data))
                    is LocalDateTime -> group.add(colName, ParquetTypeUtils.convertLocalDateTimeForParquet(data))
                    is Date -> group.add(colName, ParquetTypeUtils.convertDateForParquet(data))
                }
            }

            else -> {
                throw UnsupportedOperationException("unsupported binary type data: ${data::class.java}")
            }
        }
    }
}
