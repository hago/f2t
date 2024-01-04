/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.datafile.AvroField
import com.hagoapp.f2t.datafile.SimpleAvroSchema
import com.hagoapp.f2t.util.JDBCTypeUtils
import com.hagoapp.f2t.util.ParquetTypeUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.slf4j.LoggerFactory
import java.io.Closeable

/**
 * A parquet writer to write data from a iterator.
 *
 * @author suncjs
 * @since 0.8.5
 */
class ParquetIteratorWriter private constructor(
    private val schemaDefinition: TableDefinition<ColumnDefinition>,
    private val rowDataFeeder: Iterator<DataRow>,
    config: ParquetWriterConfig
) : Closeable {

    companion object {

        class Builder {
            private lateinit var config: ParquetWriterConfig

            private lateinit var schemaDefinition: TableDefinition<ColumnDefinition>

            private lateinit var rowDataFeeder: Iterator<DataRow>

            fun create(): ParquetIteratorWriter {
                return ParquetIteratorWriter(schemaDefinition, rowDataFeeder, config)
            }

            fun withConfig(config: ParquetWriterConfig): Builder {
                this.config = config
                return this
            }

            fun withMetadata(schemaDefinition: TableDefinition<ColumnDefinition>): Builder {
                this.schemaDefinition = schemaDefinition
                return this
            }

            fun withRowDataFeeder(rowDataFeeder: Iterator<DataRow>): Builder {
                this.rowDataFeeder = rowDataFeeder
                return this
            }
        }

        @JvmStatic
        fun createWriter(
            schemaDefinition: TableDefinition<ColumnDefinition>,
            rowDataFeeder: Iterator<DataRow>,
            config: ParquetWriterConfig
        ): ParquetIteratorWriter {
            return Builder().withConfig(config).withRowDataFeeder(rowDataFeeder)
                .withMetadata(schemaDefinition).create()
        }
    }

    private val schema: SimpleAvroSchema = SimpleAvroSchema()
    private val avroSchema: Schema
    private val writer: ParquetWriter<GenericData.Record>
    private val logger = LoggerFactory.getLogger(ParquetIteratorWriter::class.java)

    init {
        schema.type = "record"
        schema.name = config.name
        schema.fields = schemaDefinition.columns.map { col ->
            val field = AvroField()
            field.name = col.name
            field.type = ParquetTypeUtils.mapToAvroType(col.dataType)
            field
        }
        val f = LocalOutputFile(config.parquetFileName)
        val schemaValue = schema.toJson()
        avroSchema = Schema.Parser().parse(schemaValue)
        writer = AvroParquetWriter.builder<GenericData.Record>(f).withSchema(avroSchema)
            .build()
    }

    fun write() {
        while (rowDataFeeder.hasNext()) {
            val row = rowDataFeeder.next()
            val record = GenericData.Record(avroSchema)
            row.cells.forEachIndexed { i, cell ->
                val colDefinition = schemaDefinition.columns[i]
                val col = colDefinition.name
                logger.trace("write data: {}", cell.data)
                record.put(col, JDBCTypeUtils.toTypedValue(cell.data, colDefinition.dataType))
            }
            writer.write(record)
        }
    }

    override fun close() {
        writer.close()
    }
}
