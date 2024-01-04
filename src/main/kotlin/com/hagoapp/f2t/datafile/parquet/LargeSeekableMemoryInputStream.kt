/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.parquet.io.SeekableInputStream
import org.slf4j.LoggerFactory
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

/**
 * The implementation of <code>SeekableInputStream</code> to help apache hadoop's parquet file reader to deal with
 * memory content loaded from a parquet file larger than 2G. It uses an array of byte array to simulate a large
 * buffer.
 *
 * @author Chaojun Sun
 * @since 0.7.5
 */
class LargeSeekableMemoryInputStream(inputStream: InputStream, private val length: Long) :
    SeekableInputStream() {

    companion object {
        private const val DEFAULT_MEM_SLOT_SIZE = Int.MAX_VALUE / 2
        private var logger = LoggerFactory.getLogger(LargeSeekableMemoryInputStream::class.java)
    }

    private val memSlots: Array<ByteArray>
    private var position = -1L
    var memSlotSize = DEFAULT_MEM_SLOT_SIZE

    init {
        val slotNum = ((length - 1) / memSlotSize + 1).toInt()
        memSlots = Array(slotNum) { i ->
            when (i) {
                slotNum - 1 -> ByteArray((length - memSlotSize * (slotNum - 1)).toInt())
                else -> ByteArray(memSlotSize)
            }
        }
        var slotIndex = 0
        var slotPosition = 0
        while (slotIndex < slotNum) {
            val toRead = memSlots[slotIndex].size - slotPosition
            val i = inputStream.read(memSlots[slotIndex], slotPosition, toRead)
            if (i == -1) {
                break
            }
            slotPosition += i
            if (slotPosition == memSlots[slotIndex].size) {
                slotIndex++
                slotPosition = 0
            }
        }
        position = 0
    }

    override fun read(buf: ByteBuffer?): Int {
        logger.trace("read(ByteBuffer)")
        buf ?: throw IOException("byteBuffer is null")
        if (position >= length) {
            return -1
        }
        val remain = buf.remaining()
        val readable = length - position
        val shouldRead = readable.coerceAtMost(remain.toLong()).toInt()
        val bufPos = buf.position()
        copyFromInternal(shouldRead, buf.array(), buf.position())
        buf.position(bufPos + shouldRead)
        return shouldRead
    }

    /**
     * This method copies data from internal buffer area to a byte array. It does NOT check size and range of both
     * source and target buffer. User MUST check do those tasks outside this method.
     *
     * @param len         length of data to copy
     * @param target      buffer to copy to
     * @param targetStart start position of target
     */
    private fun copyFromInternal(len: Int, target: ByteArray, targetStart: Int) {
        var numToCopy = len
        var targetOffset = targetStart
        while (numToCopy > 0) {
            val slotIndex = (position / memSlotSize).toInt()
            val slotPos = (position - memSlotSize * slotIndex).toInt()
            val slotRemain = memSlots[slotIndex].size - slotPos
            val numToCopyInSlot = numToCopy.coerceAtMost(slotRemain)
            System.arraycopy(memSlots[slotIndex], slotPos, target, targetOffset, numToCopyInSlot)
            numToCopy -= numToCopyInSlot
            position += numToCopyInSlot
            targetOffset += numToCopyInSlot
        }
    }

    override fun read(): Int {
        if (position >= length) {
            return -1
        }
        val slotIndex = (position / memSlotSize).toInt()
        val slotPosition = (position - memSlotSize * slotIndex).toInt()
        val data = memSlots[slotIndex][slotPosition].toUByte()
        position += 1
        logger.trace("read() $pos return $data")
        return data.toInt()
    }

    override fun getPos(): Long {
        logger.trace("getPos() {}", position)
        return position
    }

    override fun seek(newPos: Long) {
        logger.trace("seek({})", newPos)
        if ((newPos >= length) || (newPos < 0)) {
            throw IOException("attempt to seek position $newPos, which exceeds range 0 - $length")
        }
        position = newPos
    }

    override fun reset() {
        logger.trace("reset")
        position = 0
    }

    override fun readFully(bytes: ByteArray?) {
        logger.trace("readFully(ByteArray?)")
        bytes ?: throw IOException("null ByteBuffer")
        readFully(bytes, 0, bytes.size)
    }

    override fun readFully(bytes: ByteArray?, start: Int, len: Int) {
        logger.trace("readFully(ByteArray, {}, {})", start, len)
        bytes ?: throw IOException("null bytes")
        val needToRead = len - start
        if (needToRead > length - position) {
            throw EOFException("EOF encountered, only ${length - position} bytes readable when $needToRead needed")
        }
        copyFromInternal(needToRead, bytes, start)
    }

    override fun readFully(buf: ByteBuffer?) {
        logger.trace("readFully(ByteBuffer)")
        buf ?: throw IOException("null ByteBuffer")
        val content = buf.array()
        val posBefore = position
        readFully(content, buf.position(), content.size - buf.position())
        buf.position((position - posBefore).toInt())
    }
}
