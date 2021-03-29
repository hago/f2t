/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

open class FileInfo(initFilename: String) {

    var filename: String = initFilename
        set(value) {
            field = value
            calcType()
        }
    protected lateinit var type: FileType

    init {
        calcType()
    }

    fun getFileType(): FileType {
        if (!this::type.isInitialized) {
            calcType()
        }
        return type
    }

    protected fun calcType() {
        val i = filename.lastIndexOf('.')
        type = if (i < 0) {
            FileType.Unknown
        } else when (filename.substring(i).toLowerCase()) {
            ".csv" -> FileType.CSV
            ".xls" -> FileType.Excel
            ".xlsx" -> FileType.ExcelOpenXML
            else -> FileType.Unknown
        }
    }

}
