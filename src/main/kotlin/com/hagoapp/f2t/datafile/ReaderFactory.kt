/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.datafile.csv.CSVDataReader

object ReaderFactory {
    @JvmStatic
    fun getReader(fileInfo: FileInfo): Reader {
        return when (val type = fileInfo.type) {
            FileType.CSV -> CSVDataReader()
            FileType.Excel -> TODO()
            FileType.ExcelOpenXML -> TODO()
            else -> throw F2TException("file type ${type.name} is not supported")
        }
    }
}