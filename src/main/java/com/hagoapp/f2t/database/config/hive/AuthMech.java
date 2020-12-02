/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config.hive;

import com.google.gson.annotations.SerializedName;

public enum AuthMech {
    @SerializedName("0")
    No_Authentication(0),
    @SerializedName("1")
    UserName(1),
    @SerializedName("3")
    UserName_And_Password(3);

    private int value;

    AuthMech(int i) {
        this.value = i;
    }

    public int getValue() {
        return value;
    }
}
