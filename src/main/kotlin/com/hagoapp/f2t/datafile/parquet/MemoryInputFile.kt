/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream

class MemoryInputFile(private val input: ByteArray) : InputFile {

    private val size = input.size
    //private val seekableInputStream = SeekableMemoryInputStream(input)

    override fun getLength(): Long {
        return size.toLong()
    }

    override fun newStream(): SeekableInputStream {
        return SeekableMemoryInputStream(input)
    }

    override fun toString(): String {
        return "implementation of org.apache.parquet.io.InputFile using data in memory, no actual file"
    }
}
