/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

/**
 * This class contains a cell from a row.
 *
 * @property data    the actual value of the cell
 * @property index   the cell index from its row
 * @author  Chaojun Sun
 * @since 0.2
 */
data class DataCell(
    var data: Any? = null,
    var index: Int = 0
) {
    override fun toString(): String {
        return "DataCell(data=$data, index=$index, dataType=${data?.javaClass?.canonicalName ?: "null"})"
    }
}
