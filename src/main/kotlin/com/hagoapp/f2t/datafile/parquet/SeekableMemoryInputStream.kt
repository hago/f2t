/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.parquet.io.SeekableInputStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

/**
 * The implementation of <code>SeekableInputStream</code> to help apache hadoop's parquet file reader to deal with
 * memory content loaded from a parquet file.
 *
 * @author Chaojun Sun
 * @since 0.7
 */
class SeekableMemoryInputStream(private val input: ByteArray) : SeekableInputStream() {

    companion object {

        private val logger: Logger = LoggerFactory.getLogger(SeekableMemoryInputStream::class.java)

    }

    private var pos = 0

    override fun available(): Int {
        return input.size - pos
    }

    override fun reset() {
        pos = 0
    }

    override fun read(buf: ByteBuffer?): Int {
        //logger.trace("SeekableMemoryInputStream.read(buf: ByteBuffer?)")
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
        //logger.trace("SeekableMemoryInputStream.read() $pos ${input.size}")
        if (pos >= input.size) {
            return -1
        }
        val data = input[pos].toUByte()
        pos += 1
        logger.trace("SeekableMemoryInputStream.read() $pos return $data")
        return data.toInt()
    }

    override fun getPos(): Long {
        //logger.trace("SeekableMemoryInputStream.getPos()")
        return pos.toLong()
    }

    override fun seek(newPos: Long) {
        //logger.trace("SeekableMemoryInputStream.seek($newPos)")
        if ((newPos >= input.size) || (newPos < 0)) {
            throw IOException("Exceeds seekable range")
        }
        pos = newPos.toInt()
    }

    override fun readFully(bytes: ByteArray?) {
        //logger.trace("SeekableMemoryInputStream.readFully(bytes: ByteArray?)")
        bytes ?: throw IOException("null bytes")
        readFully(bytes, 0, bytes.size)
    }

    override fun readFully(bytes: ByteArray?, start: Int, len: Int) {
        //logger.trace("SeekableMemoryInputStream.readFully(bytes: ByteArray?, start: Int, len: Int)")
        bytes ?: throw IOException("null bytes")
        if (pos + len > input.size) {
            throw EOFException("EOF encountered, only ${input.size - pos} bytes readable when $len needed")
        }
        System.arraycopy(input, pos, bytes, start, len)
        pos += len
    }

    override fun readFully(buf: ByteBuffer?) {
        //logger.trace("SeekableMemoryInputStream.readFully(buf: ByteBuffer?)")
        buf ?: throw IOException("null bytes")
        val content = buf.array()
        val posBeforeRead = pos
        readFully(content, 0, content.size)
        buf.position(pos - posBeforeRead)
    }

}
