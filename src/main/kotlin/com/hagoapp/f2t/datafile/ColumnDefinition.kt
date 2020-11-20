/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import java.sql.JDBCType

data class ColumnDefinition(
    var index: Int,
    var name: String,
    var possibleTypes: MutableSet<JDBCType> = mutableSetOf(),
    var inferredType: JDBCType? = null
)
