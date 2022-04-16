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
 * The interface defines how to transform data from some data types of file column to given types of database column.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
interface TypedColumnTransformer {
    /**
     * Transform file data in some data types into given types for database column.
     *
     * @param src   source data
     * @param fileColumnDefinition  file column definition
     * @param dbColumnDefinition    database column definition
     * @param extra additional params that may be required for special case, preserved
     * @return transformed value
     */
    fun transform(
        src: Any?,
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition,
        vararg extra: String
    ): Any?

    /**
     * Describe which source data types are supported by self.
     *
     * @return data types supported to use as source
     */
    fun supportSourceTypes(): Set<JDBCType>

    /**
     * Describe which destination data types are supported by self.
     *
     * @return data types supported to use as destination
     */
    fun supportDestinationTypes(): Set<JDBCType>
}
