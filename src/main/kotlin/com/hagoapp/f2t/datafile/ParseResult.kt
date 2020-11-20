/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import java.time.Instant

class ParseResult {
    val startTime = Instant.now().toEpochMilli()
    var endTime: Long? = null
        private set
    private val errors: MutableMap<Long, Throwable> = HashMap()
    
    fun getErrors(): Map<Long, Throwable> {
        return errors
    }

    fun end() {
        endTime = Instant.now().toEpochMilli()
    }

    fun milliSecondsUsed(): Long? {
        return if (endTime == null) null else endTime!! - startTime
    }

    val isSucceeded: Boolean
        get() = errors.isEmpty()

    fun addError(rowNo: Long, e: Throwable) {
        errors[rowNo] = e
    }
}
