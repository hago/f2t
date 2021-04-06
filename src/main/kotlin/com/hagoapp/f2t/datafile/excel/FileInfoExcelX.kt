/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.hagoapp.f2t.datafile.excel

class FileInfoExcelX : FileInfoExcel() {
    companion object {
        const val FILE_TYPE_EXCEL_OPEN_XML = 3
    }

    override fun getFileTypeValue(): Int {
        return FILE_TYPE_EXCEL_OPEN_XML
    }

    override fun getSupportedFileExtNames(): Set<String> {
        return setOf("xlsx")
    }
}
