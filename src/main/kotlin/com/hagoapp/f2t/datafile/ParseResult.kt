/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import java.time.Instant

/**
 * Result definition if parsing.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
class ParseResult {
    /**
     * The starting time of parsing.
     */
    val startTime = Instant.now().toEpochMilli()

    /**
     * The end time of parsing, null if parsing is still in processing.
     */
    var endTime: Long? = null
        private set
    private val errors: MutableMap<Long, Throwable> = HashMap()

    /**
     * Get errors per lines.
     *
     * @return a map whose key is line number and value is errors thrown when parsing this line.
     */
    fun getErrors(): Map<Long, Throwable> {
        return errors
    }

    /**
     * End parsing.
     */
    fun end() {
        endTime = Instant.now().toEpochMilli()
    }

    /**
     * Get time used for whole parsing.
     *
     * @return time in milliseconds
     */
    fun milliSecondsUsed(): Long? {
        return if (endTime == null) null else endTime!! - startTime
    }

    /**
     * Whether the parsing is successful.
     */
    val isSucceeded: Boolean
        get() = errors.isEmpty()

    /**
     * This method is used by caller to add erros.
     *
     * @param rowNo row number
     * @param e error
     */
    fun addError(rowNo: Long, e: Throwable) {
        errors[rowNo] = e
    }

    override fun toString(): String {
        return "ParseResult(startTime=$startTime, endTime=$endTime, errors=$errors)"
    }


}
