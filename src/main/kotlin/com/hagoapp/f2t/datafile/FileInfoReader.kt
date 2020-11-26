/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.google.gson.GsonBuilder
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.datafile.csv.FileInfoCsv
import com.hagoapp.f2t.datafile.excel.FileInfoExcel
import java.io.FileInputStream
import java.io.InputStream

class FileInfoReader {
    companion object {
        @JvmStatic
        fun createFileInfo(stream: InputStream): FileInfo {
            return createFileInfo(stream.readAllBytes())
        }

        @JvmStatic
        fun createFileInfo(content: ByteArray): FileInfo {
            return json2FileInfo(String(content))
        }

        @JvmStatic
        fun createFileInfo(filename: String): FileInfo {
            try {
                FileInputStream(filename).use {
                    return createFileInfo(it)
                }
            } catch (e: Throwable) {
                throw F2TException("Load FileInfo object from file $filename failed", e)
            }
        }

        @JvmStatic
        fun json2FileInfo(content: String): FileInfo {
            val gson = GsonBuilder().create()
            val base = gson.fromJson(content, FileInfo::class.java)
            return when (base.type) {
                FileType.CSV -> gson.fromJson(content, FileInfoCsv::class.java)
                FileType.Excel, FileType.ExcelOpenXML -> gson.fromJson(content, FileInfoExcel::class.java)
                else -> throw F2TException("FileInfo type unknown")
            }
        }
    }
}
