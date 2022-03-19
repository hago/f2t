/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.FileColumnDefinition
import java.sql.JDBCType

class FileTypeDeterminer(private val defaultDeterminer: FileColumnTypeDeterminer) {

    private val exceptions = mutableMapOf<String, FileColumnTypeDeterminer>()

    fun determinerForColumn(columnName: String, determiner: FileColumnTypeDeterminer): FileTypeDeterminer {
        exceptions[columnName] = determiner
        return this
    }

    fun determineType(fileColumnDefinition: FileColumnDefinition): JDBCType {
        return determineType(fileColumnDefinition.name, fileColumnDefinition)
    }

    fun determineType(columnName: String, fileColumnDefinition: FileColumnDefinition?): JDBCType {
        val determiner = exceptions[columnName]
        return determiner?.determineType(fileColumnDefinition) ?: defaultDeterminer.determineType(fileColumnDefinition)
    }
}
