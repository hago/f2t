/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.csv

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.datafile.FileInfo
import java.nio.charset.Charset
import java.nio.charset.UnsupportedCharsetException

/**
 * CSV file information class, inherited from <code>FileInfo</code>.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
class FileInfoCsv : FileInfo() {

    companion object {
        const val FILE_TYPE_CSV = 1
    }

    /**
     * Text encoding for CSV file.
     */
    var encoding: String? = null
        set(value) {
            try {
                Charset.forName(value)
                field = value
            } catch (e: UnsupportedCharsetException) {
                throw F2TException("$value is not valid charset", e)
            }
        }

    /**
     * Quote character to wrap a field in this file, if any. Double quote is default.
     */
    var quote: Char? = '"'

    /**
     * delimiter character to separate fields.
     */
    var delimiter: Char = ','
    override fun getFileTypeValue(): Int {
        return FILE_TYPE_CSV
    }

    override fun getSupportedFileExtNames(): Set<String> {
        return setOf("csv", "tsv")
    }

    override fun toString(): String {
        return "FileInfoCsv(encoding=$encoding, quote=$quote, delimiter=$delimiter, filename=$filename)"
    }

}
