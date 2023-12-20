/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.datafile.FileInfoReader
import com.hagoapp.f2t.datafile.csv.FileInfoCsv
import com.hagoapp.f2t.datafile.excel.FileInfoExcel
import com.hagoapp.f2t.datafile.excel.FileInfoExcelX
import com.hagoapp.f2t.datafile.parquet.FileInfoParquet
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class FileInfoReaderTest {

    private val cases = listOf(
        Pair(FileInfoCsv::class.java, "tests/process/shuihucsv.json"),
        Pair(FileInfoCsv::class.java, "tests/process/shuihucsven.json"),
        Pair(FileInfoExcel::class.java, "tests/process/shuihuxls.json"),
        Pair(FileInfoExcelX::class.java, "tests/process/shuihuxlsx.json"),
        Pair(FileInfoParquet::class.java, "tests/process/shuihuparquet.json")
    )

    @Test
    fun testCreateFileInfo() {
        for (c in cases) {
            val info = FileInfoReader.createFileInfo(c.second)
            Assertions.assertEquals(c.first, info::class.java)
        }
    }
}
