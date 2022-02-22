/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition

interface ColumnComparator {
    fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition
    ): CompareColumnResult

    companion object {
        fun create(fileColumnDefinition: FileColumnDefinition, dbColumnDefinition: ColumnDefinition): ColumnComparator {
            TODO()
        }
    }
}
