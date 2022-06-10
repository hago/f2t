/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import java.io.InputStream
import kotlin.math.pow

class ParquetStreamReader(private val input: InputStream) {

    companion object {
        val magic = ByteArray(4) { i ->
            when (i) {
                0 -> 0x50
                1 -> 0x41
                2 -> 0x52
                3 -> 0x31
                else -> throw RuntimeException("No way")
            }
        }

        private fun compareByteArray(a: ByteArray, b: ByteArray): Boolean {
            return when {
                a.size != b.size -> false
                else -> a.mapIndexed { i, elem -> elem == b[i] }.all { it }
            }
        }

        private fun littleEndianToNum(b: ByteArray): Int {
            return b.mapIndexed { i, v ->
                (256.0.pow(i) * v).toInt()
            }.sum()
        }
    }

    init {
        val head = input.readNBytes(4)
        if (!compareByteArray(magic, head)) {
            throw java.lang.IllegalArgumentException("Not a parquet, bad head")
        }
        val l = input.available()
        input.skip(l - 8L)
        val tail = input.readNBytes(8)
        if (!compareByteArray(magic, ByteArray(4) { i -> tail[i + 4] })) {
            throw java.lang.IllegalArgumentException("Not a parquet, bad tail")
        }
        val metaLength = littleEndianToNum(ByteArray(4) { i -> tail[i] })
        println(metaLength)
    }

    fun getSchema() {

    }
}
