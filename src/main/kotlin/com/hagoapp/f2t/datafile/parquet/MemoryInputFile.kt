/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.IOException
import java.io.InputStream

/**
 * The InputFile implementation to deal with file content in memory.
 *
 * @author Chaojun Sun
 * @since 0.7
 */
class MemoryInputFile private constructor() : InputFile {

    private lateinit var inputStream: InputStream
    private var length: Long = -1

    constructor(inputStream: InputStream, length: Long) : this() {
        this.inputStream = inputStream
        this.length = length
    }

    override fun getLength(): Long {
        return length
    }

    override fun newStream(): SeekableInputStream {
        return if (this::inputStream.isInitialized) {
            LargeSeekableMemoryInputStream(inputStream, length)
        } else {
            throw IOException("No content")
        }
    }

    override fun toString(): String {
        return "virtual file implementation of org.apache.parquet.io.InputFile using data in memory, size $length"
    }
}
