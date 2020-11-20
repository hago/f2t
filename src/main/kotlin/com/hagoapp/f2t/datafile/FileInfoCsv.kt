/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import java.nio.charset.StandardCharsets

class FileInfoCsv(filename: String) : FileInfo(filename) {
    var encoding = StandardCharsets.UTF_8.displayName()
    var quote = '"'
    var delimiter = ","
}
