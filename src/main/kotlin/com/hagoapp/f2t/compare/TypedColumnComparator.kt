/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import java.sql.JDBCType

/**
 * The interface defines feasibility check and possibility of data loss between 2 data types from a file column to
 * database column.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
interface TypedColumnComparator {
    /**
     * Whether data can be load from file column in source definition into database column in target definition.
     *
     * @param fileColumnDefinition  file column definition
     * @param dbColumnDefinition    database column definition
     * @param extra additional information that may be required for some special cases, preserved
     * @return A result to indicate many flags
     */
    fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): CompareColumnResult

    /**
     * Describe which source data types are supported by self.
     *
     * @return data types supported to use as source
     */
    fun supportSourceTypes(): Set<JDBCType>

    /**
     * Describe which target types are supported by self.
     *
     * @return data types supported to use as destination
     */
    fun supportDestinationTypes(): Set<JDBCType>

}
