/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database

/**
 * This class contains full table identifier, including schema and name.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
data class TableName(
    var tableName: String,
    var schema: String = ""
)
