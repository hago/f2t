/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.fieldsetter

import com.hagoapp.f2t.database.DbFieldSetter
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types
import java.time.ZonedDateTime

class TimestampFieldSetter : DbFieldSetter() {

    override fun set(stmt: PreparedStatement, i: Int, value: Any?) {
        val newValue = if (transformer == null) value else transformer.transform(value)
        if (newValue != null) stmt.setTimestamp(i, Timestamp.from((newValue as ZonedDateTime).toInstant()))
        else stmt.setNull(i, Types.TIMESTAMP_WITH_TIMEZONE)
    }
}