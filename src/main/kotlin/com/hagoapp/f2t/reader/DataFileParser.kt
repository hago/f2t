/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.reader;

import com.hagoapp.f2t.datafile.ColumnsDefinition
import com.hagoapp.f2t.datafile.DataRow
import java.io.Closeable
import java.io.InputStream

interface DataFileParser : Closeable, Iterator<DataRow> {
    fun open(inputStream: InputStream)
    fun parseColumns(): ColumnsDefinition
    fun currentRowNo(): Long
}
