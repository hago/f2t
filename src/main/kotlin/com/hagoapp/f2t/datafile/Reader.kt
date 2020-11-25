/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.DataRow
import java.io.Closeable

interface Reader : Closeable, MutableIterator<DataRow> {
    fun open(fileInfo: FileInfo)
    fun findColumns(): List<ColumnDefinition>
    fun inferColumnTypes(sampleRowCount: Long = -1): List<ColumnDefinition>
}