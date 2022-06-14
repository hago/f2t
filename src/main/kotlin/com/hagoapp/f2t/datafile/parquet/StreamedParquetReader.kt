/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.Closeable
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

class StreamedParquetReader(input: InputStream) : Closeable {

    companion object {
        class StreamedInputFile(input: InputStream, explicitLength: Long? = null) : InputFile {

            private val size = explicitLength ?: input.available().toLong()
            private val seekableInputStream = SeekableFileInputStream(input)

            override fun getLength(): Long {
                return size
            }

            override fun newStream(): SeekableInputStream {
                return seekableInputStream
            }

        }

        class SeekableFileInputStream(private val input: InputStream) : SeekableInputStream() {

            private var pos: Long = 0

            override fun read(buf: ByteBuffer?): Int {
                buf ?: throw IOException("buffer is null")
                val remain = buf.remaining()
                val actualRead = input.readNBytes(buf.array(), buf.capacity() - remain, remain)
                pos += actualRead
                return actualRead
            }

            override fun read(): Int {
                val actualRead = input.read()
                pos += actualRead
                return actualRead
            }

            override fun getPos(): Long {
                return pos
            }

            override fun seek(newPos: Long) {
                input.reset()
                input.skip(newPos)
                pos = newPos
            }

            override fun readFully(bytes: ByteArray?) {
                bytes ?: throw IOException("null bytes")
                readFully(bytes, 0, bytes.size)
            }

            override fun readFully(bytes: ByteArray?, start: Int, len: Int) {
                bytes ?: throw IOException("null bytes")
                val actualRead = input.read(bytes, start, len)
                pos += actualRead
                if (actualRead < len) {
                    throw EOFException("EOF encountered, only $actualRead bytes read when $len needed")
                }
            }

            override fun readFully(buf: ByteBuffer?) {
                buf ?: throw IOException("null bytes")
                val content = buf.array()
                readFully(content, 0, content.size)
            }

        }
    }

    private val row0: List<RecordCell>
    val columns: List<String>
    private var rowNo = 0L
    private val reader: ParquetReader<GenericData.Record>

    init {
        val inputFile = StreamedInputFile(input)
        reader = AvroParquetReader.builder<GenericData.Record>(inputFile).build()
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
