/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.datafile.ParseResult

/**
 * Execution result of a file to table process.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
class F2TResult {

    /**
     * row count processed.
     */
    var rowCount: Int = 0
        set(value) {
            if (finished) {
                throw F2TException("Result is already final")
            } else {
                field = value
            }
        }

    /**
     * Definition of final target table, created or existed.
     */
    var tableDefinition: TableDefinition<ColumnDefinition>? = null
        set(value) {
            if (finished) {
                throw F2TException("Result is already final")
            } else {
                field = value
            }
        }

    /**
     * Errors occurred during processing.
     */
    val errors: MutableList<Throwable> = mutableListOf()
    private var finished = false

    /**
     * File parsing result detail.
     */
    lateinit var parseResult: ParseResult
        private set

    /**
     * Finalize this result.
     */
    fun complete(parseResult: ParseResult) {
        this.parseResult = parseResult
        finished = true
    }

    /**
     * Get the success status of this result. An exception will be thrown is processing is not complete.
     *
     * @return true if completed successfully, otherwise false
     * @throws F2TException if processing is not completed when invoking
     */
    fun succeeded(): Boolean {
        return when {
            !finished -> throw F2TException("not started")
            errors.isNotEmpty() -> false
            else -> true
        }
    }

    override fun toString(): String {
        return "F2TResult(rowCount=$rowCount, tableDefinition=$tableDefinition, errors=$errors, finished=$finished, parseResult=$parseResult)"
    }

}
