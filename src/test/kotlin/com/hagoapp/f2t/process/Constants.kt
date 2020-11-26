/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.process

class Constants {
    companion object {
        const val DATABASE_CONFIG_FILE = "f2t.db"
        const val PROCESS_CONFIG_FILE = "f2t.process"
        const val FILE_CONFIG_FILE = "f2t.file"

        val configDescriptions = mapOf(
            DATABASE_CONFIG_FILE to "config file of target database",
            PROCESS_CONFIG_FILE to "config file of f2t process",
            FILE_CONFIG_FILE to "config file of file to table process"
        )
    }
}
