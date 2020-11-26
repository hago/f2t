/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.process

import com.hagoapp.f2t.DataRow
import com.hagoapp.f2t.F2TLogger
import com.hagoapp.f2t.ProcessObserver

class TestProcessObserver : ProcessObserver {
    private val logger = F2TLogger.getLogger()
    override fun onRowRead(row: DataRow) {
        logger.debug("row ${row.rowNo}")
    }
}
