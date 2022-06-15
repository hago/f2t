/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

/**
 * A class to represent a data cell read from parquet, containing its field name and value.
 *
 * @author Chaojun Sun
 * @since 0.7
 */
class RecordCell(
    val fieldName: String,
    val value: Any?
)
