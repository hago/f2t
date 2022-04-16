/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

/**
 * Compare result between file column and table column.
 *
 * @property isTypeMatched whether the 2 types match, which means data can copy without transforming
 * @property canLoadDataFrom    whether data copying is possible with certain transforming
 * @author Chaojun Sun
 * @since 0.6
 */
data class CompareColumnResult(
    val isTypeMatched: Boolean,
    val canLoadDataFrom: Boolean
)
