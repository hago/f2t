/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

class DataCell<T> {
    var data: T? = null
        private set
    var index = 0

    fun setData(data: T?) {
        this.data = data
    }
}
