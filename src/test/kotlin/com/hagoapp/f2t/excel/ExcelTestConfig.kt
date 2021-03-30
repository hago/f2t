/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.excel

import com.hagoapp.f2t.FileTestExpect
import com.hagoapp.f2t.datafile.excel.FileInfoExcel

data class ExcelTestConfig(
    var fileInfo: FileInfoExcel,
    var expect: FileTestExpect
)
