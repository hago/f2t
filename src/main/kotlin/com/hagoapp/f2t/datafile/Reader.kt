/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.FileColumnDefinition
import java.io.Closeable

/**
 * The interface to define operations required to read data from certain type of file.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
interface Reader : Closeable, Iterator<DataRow> {
    /**
     * Open data file. This method will be called prior to any file operations.
     *
     * @param fileInfo  source file information
     */
    fun open(fileInfo: FileInfo)

    /**
     * This method should return row count of this file.
     *
     * @return row count, or null if not determined yet
     */
    fun getRowCount(): Int?

    /**
     * This method should return column definitions from data file, and definitions at this moment should
     * have all column names determined at least.
     *
     * @return list of file column definitions in same order with file
     */
    fun findColumns(): List<FileColumnDefinition>

    /**
     * Infer file column types using specified count of rows from data file and return column definitions
     * with inferred type information.
     *
     * @param sampleRowCount    how many rows the implementation should look over to determine types, -1 means
     * using all rows.
     * @return list of file column definitions in same order with file
     */
    fun inferColumnTypes(sampleRowCount: Long = -1): List<FileColumnDefinition>

    /**
     * Return a list of types of data file supported.
     *
     * @return a list of types of data file supported.
     */
    fun getSupportedFileType(): Set<Int>

    /**
     * A builder style method to set a customized <code>FileTypeDeterminer</code>, which will be used to
     * determine column types instead of built-in one.
     *
     * @return reader itself
     */
    fun setupTypeDeterminer(determiner: FileTypeDeterminer): Reader

    /**
     * A builder style method to set up whether skip type infer process. Skipping it can save processing time
     * when type information is not needed.
     */
    fun skipTypeInfer(): Reader
}
