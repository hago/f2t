/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

/**
 * Base file information class to store data file and particular options.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
open class FileInfo {

    companion object {
        const val FILE_TYPE_UNDETERMINED = 0
    }

    /**
     * File name with path.
     */
    var filename: String? = null

    /**
     * The type of file, this value will be used to find reader.
     */
    val type: Int = getFileTypeValue()

    /**
     * Get file type. This method must be override in descendants to provider correct file type value.
     *
     * @return file type
     */
    open fun getFileTypeValue(): Int {
        return FILE_TYPE_UNDETERMINED
    }

    /**
     * Get extension names of supported type of files.
     *
     * @return extension names of supported type of files
     */
    open fun getSupportedFileExtNames(): Set<String> {
        throw NotImplementedError("No supported file extension names in base class FileInfo")
    }
}
