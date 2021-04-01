/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

open class FileInfo {

    var filename: String? = null

    open val type: Int = 0

    open fun getSupportedFileExtNames(): Set<String> {
        throw NotImplementedError("No supported file extension names in base class FileInfo")
    }
}
