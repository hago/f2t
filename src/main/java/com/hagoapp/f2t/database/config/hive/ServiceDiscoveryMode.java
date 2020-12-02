/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.config.hive;

import com.google.gson.annotations.SerializedName;

public enum ServiceDiscoveryMode {
    @SerializedName("0")
    No_Service_Discovery,
    @SerializedName("1")
    ZooKeeper;
}
