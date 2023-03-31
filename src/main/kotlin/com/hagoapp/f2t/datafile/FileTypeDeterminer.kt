/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.FileColumnDefinition
import java.sql.JDBCType

/**
 * A class to determine corresponding JDBC types for all columns from data file.
 *
 * @property defaultDeterminer default, preset column type determiner for all columns
 * @constructor create a file type determiner with 1 column type determiner for all columns
 * @author Chaojun Sun
 * @since 0.6
 */
class FileTypeDeterminer(private val defaultDeterminer: FileColumnTypeDeterminer) {

    private val exceptions = mutableMapOf<String, FileColumnTypeDeterminer>()

    /**
     * Builder style method to set up a column type determiner for specified column.
     *
     * @param columnName    column name
     * @param determiner    type determiner for this column
     * @return instance itself
     */
    fun determinerForColumn(columnName: String, determiner: FileColumnTypeDeterminer): FileTypeDeterminer {
        exceptions[columnName] = determiner
        return this
    }

    /**
     * Determine JDBC type for input column. The determiner to be used is found by column name, or default one if
     * not found.
     *
     * @param fileColumnDefinition  column definition
     * @return JDBC type
     */
    fun determineType(fileColumnDefinition: FileColumnDefinition): JDBCType {
        return determineType(fileColumnDefinition.name, fileColumnDefinition)
    }

    /**
     * Determine JDBC type for input column. The determiner to be used is found by column name, or default one if
     * not found.
     *
     * @param columnName    column name to be used to find determiner
     * @param fileColumnDefinition  column definition to be used as context
     * @return JDBC type
     */
    fun determineType(columnName: String, fileColumnDefinition: FileColumnDefinition): JDBCType {
        val determiner = exceptions[columnName]
        return determiner?.determineType(fileColumnDefinition) ?: defaultDeterminer.determineType(fileColumnDefinition)
    }

}
