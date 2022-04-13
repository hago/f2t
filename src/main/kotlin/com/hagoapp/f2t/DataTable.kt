/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

/**
 * This class is a wrapper to contain definition and data of a table.
 *
 * @param T type if column definition
 * @property columnDefinition   column definitions of the table
 * @property rows   data of the table
 * @author Chaojun Sun
 * @since 0.2
 */
data class DataTable<T : ColumnDefinition>(
    var columnDefinition: List<T>,
    var rows: List<DataRow>
)
