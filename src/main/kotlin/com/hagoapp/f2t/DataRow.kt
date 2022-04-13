/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

/**
 * This class contains a row of a data table.
 *
 * @property rowNo  row index in whole data set
 * @property cells  data cells in this row
 * @author Chaojun Sun
 * @since 0.2
 */
data class DataRow(
    var rowNo: Long = 0,
    val cells: List<DataCell>
)
