/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

import com.hagoapp.f2t.datafile.FileInfo

class FileInfoParquet : FileInfo() {

    companion object {
        const val FILE_TYPE_PARQUET = 4
    }

    override fun getFileTypeValue(): Int {
        return FILE_TYPE_PARQUET
    }

    override fun getSupportedFileExtNames(): Set<String> {
        return setOf("parquet")
    }
}
