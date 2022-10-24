/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.parquet.io.SeekableInputStream
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
        private const val MEM_SLOT_SIZE = Int.MAX_VALUE
    }

    private val memSlots: Array<ByteArray>
    private var position = -1L

    init {
        val slotNum = (length / MEM_SLOT_SIZE).toInt()
        memSlots = Array(slotNum) { i ->
            when (i) {
                slotNum - 1 -> ByteArray((length - MEM_SLOT_SIZE * (slotNum - 1)).toInt())
                else -> ByteArray(MEM_SLOT_SIZE)
            }
        }
        var slotIndex = 0
        var slotPosition = 0
        while (slotIndex < slotNum) {
            var toRead = memSlots[slotIndex].size - slotPosition
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
        TODO("Not yet implemented")
    }

    override fun read(): Int {
        TODO("Not yet implemented")
    }

    override fun getPos(): Long {
        TODO("Not yet implemented")
    }

    override fun seek(newPos: Long) {
        TODO("Not yet implemented")
    }

    override fun reset() {
        super.reset()
    }

    override fun readFully(bytes: ByteArray?) {
        TODO("Not yet implemented")
    }

    override fun readFully(bytes: ByteArray?, start: Int, len: Int) {
        TODO("Not yet implemented")
    }

    override fun readFully(buf: ByteBuffer?) {
        TODO("Not yet implemented")
    }
}