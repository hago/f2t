/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.FileInfo;
import com.hagoapp.f2t.datafile.ParseResult;
import com.hagoapp.util.StackTraceWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An interface to define behaviors those a watcher for data file parsing should have. Each notification has
 * and empty implementation. Any class implemented this interface is allowed to implement only interested actions.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
public interface ParseObserver {

    /**
     * Notified when parsing starts.
     *
     * @param fileInfo the information of the file to be parsed
     */
    default void onParseStart(@NotNull FileInfo fileInfo) {
    }

    /**
     * Notified when all column names of the file is determined. At this moment, only <code>name</code> and
     * <code>order</code> fields are filled for each <code>FileColumnDefinition</code> in list.
     *
     * @param columnDefinitionList column definitions in natural order from the file
     */
    default void onColumnsParsed(@NotNull List<FileColumnDefinition> columnDefinitionList) {
    }

    /**
     * Notified when all column information from the file are inferred, which means a list of possible data types
     * and the most possible data type fields are filled, as well as other fields to describe features of data.
     *
     * @param columnDefinitionList column definitions in natural order from the file
     */
    default void onColumnTypeDetermined(@NotNull List<FileColumnDefinition> columnDefinitionList) {
    }

    /**
     * Notified when a row is read. From this row object, row number and a list of data cell could be found.
     * Cells appear in the natural order from file and the data of cell could be typed or string, depending
     * on actual original file type(some file type stores type information, other don't).
     *
     * @param row data row
     */
    default void onRowRead(@NotNull DataRow row) {
    }

    /**
     * Notified when parsing is complete with the basic file information and a <code>ParseResult</code> result.
     *
     * @param fileInfo file information
     * @param result   parsing result
     */
    default void onParseComplete(@NotNull FileInfo fileInfo, @NotNull ParseResult result) {
    }

    /**
     * Notified when error occurs while reading a row.
     *
     * @param e the error detail
     * @return true if parsing should continue, or false to stop parsing
     */
    default boolean onRowError(@NotNull Throwable e) {
        return true;
    }

    /**
     * Notified when count of rows is determined.
     *
     * @param rowCount count of rows.
     */
    default void onRowCountDetermined(int rowCount) {
    }

    /**
     * Notified when error occurs while opening file or analysing metadata of file. Paring can't proceed from
     * if this happened.
     *
     * @param e error detail
     */
    default void onError(@NotNull Throwable e) {
        var logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
        logger.error(e.getMessage());
        StackTraceWriter.writeToLogger(e, logger);
    }
}
