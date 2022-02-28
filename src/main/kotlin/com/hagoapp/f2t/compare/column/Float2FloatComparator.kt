/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare.column

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import com.hagoapp.f2t.compare.ColumnComparator
import com.hagoapp.f2t.compare.CompareColumnResult
import java.sql.JDBCType

class Float2FloatComparator : ColumnComparator.Comparator {
    override fun dataCanLoadFrom(
        fileColumnDefinition: FileColumnDefinition,
        dbColumnDefinition: ColumnDefinition
    ): CompareColumnResult {
        return CompareColumnResult(
            isTypeMatched = true,
            fileColumnDefinition.typeModifier.precision <= dbColumnDefinition.typeModifier.precision &&
                    fileColumnDefinition.typeModifier.scale <= dbColumnDefinition.typeModifier.scale
        )
    }

    override fun supportSourceTypes(): Set<JDBCType> {
        return setOf(JDBCType.FLOAT, JDBCType.DOUBLE, JDBCType.DECIMAL)
    }

    override fun supportDestinationTypes(): Set<JDBCType> {
        return setOf(JDBCType.FLOAT, JDBCType.DOUBLE, JDBCType.DECIMAL)
    }
}
