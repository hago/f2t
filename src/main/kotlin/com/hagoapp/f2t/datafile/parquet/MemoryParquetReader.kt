/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.F2TLogger
import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.slf4j.Logger
import java.io.Closeable
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

class MemoryParquetReader(input: ByteArray) : Closeable {

    companion object {

        private val logger: Logger = F2TLogger.getLogger()

        class MemoryInputFile(input: ByteArray) : InputFile {

            private val size = input.size
            private val seekableInputStream = SeekableMemoryInputStream(input)

            override fun getLength(): Long {
                return size.toLong()
            }

            override fun newStream(): SeekableInputStream {
                return seekableInputStream
            }

        }

        class SeekableMemoryInputStream(private val input: ByteArray) : SeekableInputStream() {

            private var pos = 0

            override fun read(buf: ByteBuffer?): Int {
                buf ?: throw IOException("buffer is null")
                if (pos >= input.size) {
                    return -1
                }
                val remain = buf.remaining()
                val readable = input.size - pos
                val shouldRead = if (remain >= readable) readable else remain
                System.arraycopy(input, pos, buf.array(), 0, shouldRead)
                pos += shouldRead
                return shouldRead
            }

            override fun read(): Int {
                if (pos >= input.size) {
                    return -1
                }
                val data = input[pos]
                pos += 1
                return data.toInt()
            }

            override fun getPos(): Long {
                return pos.toLong()
            }

            override fun seek(newPos: Long) {
                logger.debug("seek: newPos")
                if ((newPos >= input.size) || (newPos < 0)) {
                    throw IOException("Exceeds seekable range")
                }
                pos = newPos.toInt()
            }

            override fun readFully(bytes: ByteArray?) {
                bytes ?: throw IOException("null bytes")
                readFully(bytes, 0, bytes.size)
            }

            override fun readFully(bytes: ByteArray?, start: Int, len: Int) {
                bytes ?: throw IOException("null bytes")
                if (pos + len >= input.size) {
                    throw EOFException("EOF encountered, only ${input.size - pos} bytes readable when $len needed")
                }
                System.arraycopy(input, pos, bytes, start, len)
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
        val inputFile = MemoryInputFile(input)
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
