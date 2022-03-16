/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.datafile.DataTypeDeterminer
import com.hagoapp.f2t.datafile.FileInfo
import com.hagoapp.f2t.datafile.Reader

class ParquetFileReader : Reader {
    private lateinit var parquetFile: FileInfoParquet

    override fun open(fileInfo: FileInfo) {
        if (fileInfo !is FileInfoParquet) {
            throw java.lang.UnsupportedOperationException("not a parquet config")
        }
        parquetFile = fileInfo
    }

    override fun getRowCount(): Int? {
        TODO("Not yet implemented")
    }

    override fun findColumns(): List<FileColumnDefinition> {
        TODO("Not yet implemented")
    }

    override fun inferColumnTypes(sampleRowCount: Long): List<FileColumnDefinition> {
        TODO("Not yet implemented")
    }

    override fun getSupportedFileType(): Set<Int> {
        return setOf(FileInfoParquet.FILE_TYPE_PARQUET)
    }

    override fun setupTypeDeterminer(determiner: DataTypeDeterminer): Reader {
        TODO("Not yet implemented")
    }

    override fun setupColumnTypeDeterminer(column: String, determiner: DataTypeDeterminer): Reader {
        TODO("Not yet implemented")
    }

    override fun skipTypeInfer(): Reader {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun hasNext(): Boolean {
        TODO("Not yet implemented")
    }

    override fun next(): DataRow {
        TODO("Not yet implemented")
    }
}
