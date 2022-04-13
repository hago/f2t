/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.util

import org.slf4j.Logger

/**
 * A convenient tool to log error's stacktrace.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class StackTraceWriter {
    companion object {
        @JvmStatic
        fun writeToLogger(throwable: Throwable, logger: Logger) {
            logger.error("Exception thrown: ${throwable.message}")
            throwable.stackTrace.forEach { logger.error("\t{}", it.toString()) }
        }
    }
}
