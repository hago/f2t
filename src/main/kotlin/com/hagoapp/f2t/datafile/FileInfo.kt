/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

open class FileInfo {

    companion object {
        const val FILE_TYPE_UNDETERMINED = 0
    }

    var filename: String? = null

    val type: Int = getFileTypeValue()

    open fun getFileTypeValue(): Int {
        return FILE_TYPE_UNDETERMINED
    }

    open fun getSupportedFileExtNames(): Set<String> {
        throw NotImplementedError("No supported file extension names in base class FileInfo")
    }
}
