/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.TableDefinition
import com.hagoapp.f2t.TableDefinitionDifference

interface TableDefinitionComparator<T : ColumnDefinition, R : ColumnDefinition> {
    fun isColumnsIdentical(a: TableDefinition<T>, b: TableDefinition<R>): Boolean
    fun canFeed(a: TableDefinition<T>, b: TableDefinition<R>): Pair<Boolean, TableDefinitionDifference>
}
