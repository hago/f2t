/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.F2TException
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

class FileInfoCsv(filename: String) : FileInfo(filename) {
    var encoding: String = StandardCharsets.UTF_8.displayName()
        set(value) {
            try {
                Charset.forName(value)
            } catch (e: Throwable) {
                throw F2TException("$value is not valid charset", e)
            }
        }
    var quote = '"'
    var delimiter = ','
}
