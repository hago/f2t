/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import java.sql.JDBCType

/**
 * Differences between 2 table definitions.
 * <code>superfluousColumns</code> means columns that exists in table 1 and is not found in table 2;
 * <code>missingColumns</code> means columns that is not found in table 1 and exists in table 2;
 * <code>typeMismatchColumns</code> means columns that exists in both tables but their types differ, each item of the
 * list is a triplet of which 1st element is column name, 2nd element is type from table 1 and 3rd element is from
 * table 2.
 */
class TableDefinitionDifference(
    var superfluousColumns: List<String>,
    var missingColumns: List<String>,
    var typeMismatchColumns: List<Triple<String, JDBCType?, JDBCType?>>
) {
    val noDifference: Boolean
        get() {
            return superfluousColumns.isEmpty() && missingColumns.isEmpty() && typeMismatchColumns.isEmpty()
        }
}
