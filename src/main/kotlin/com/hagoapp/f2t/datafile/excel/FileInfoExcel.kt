/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.hagoapp.f2t.datafile.excel

import com.hagoapp.f2t.datafile.FileInfo

/**
 * Information of excel to read. {@code}sheetIndex priorities {@code sheetName}. Sheet 0 will be used if both are null.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
open class FileInfoExcel : FileInfo() {

    companion object {
        const val FILE_TYPE_EXCEL = 2
    }

    /**
     * index of sheet to read, prior to <code>sheetName</code>.
     */
    var sheetIndex: Int? = null

    /**
     * name of sheet to read. ignored if <code>sheetIndex</code> is set.
     */
    var sheetName: String? = null
    override fun getFileTypeValue(): Int {
        return FILE_TYPE_EXCEL
    }

    override fun getSupportedFileExtNames(): Set<String> {
        return setOf("xls")
    }
}
