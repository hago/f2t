/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t

import com.hagoapp.f2t.datafile.ParseResult

class F2TResult {

    var rowCount: Int = 0
        set(value) {
            if (finished) {
                throw F2TException("Result is already final")
            } else {
                field = value
            }
        }
    var tableDefinition: TableDefinition<ColumnDefinition>? = null
        set(value) {
            if (finished) {
                throw F2TException("Result is already final")
            } else {
                field = value
            }
        }
    val errors: MutableList<Throwable> = mutableListOf()
    private var finished = false
    lateinit var parseResult: ParseResult
        private set

    fun complete(parseResult: ParseResult) {
        this.parseResult = parseResult
        finished = true
    }

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
