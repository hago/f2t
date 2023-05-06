/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.fieldsetter

import com.hagoapp.f2t.database.DbFieldSetter
import java.sql.PreparedStatement
import java.sql.Types

class BigIntFieldSetter : DbFieldSetter() {

    override fun set(stmt: PreparedStatement, i: Int, value: Any?) {
        val newValue = if (transformer == null) value else transformer.transform(value)
        if (newValue != null) stmt.setLong(i, newValue as Long) else stmt.setNull(i, Types.BIGINT)
    }
}