/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.datafile.LocalFilePositionOutputStream
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream

/**
 * A simple implementation of Hadoop's <code>OutputFile</code>, is required to write parquet file
 * to local file system.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class LocalOutputFile(private val fileName: String) : OutputFile {

    override fun create(blockSizeHint: Long): PositionOutputStream {
        return LocalFilePositionOutputStream(fileName);
    }

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
        return LocalFilePositionOutputStream(fileName);
    }

    override fun supportsBlockSize(): Boolean {
        return true
    }

    override fun defaultBlockSize(): Long {
        return 4096L
    }

    override fun getPath(): String {
        return fileName
    }
}
