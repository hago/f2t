/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

enum class FileType {
    Unknown, CSV, Excel, ExcelOpenXML;

    companion object {
        fun getFromExtension(ext: String): FileType {
            return when (ext.trim { it <= ' ' }.toLowerCase()) {
                "csv" -> CSV
                "xls" -> Excel
                "xlsx" -> ExcelOpenXML
                else -> Unknown
            }
        }
    }
}
