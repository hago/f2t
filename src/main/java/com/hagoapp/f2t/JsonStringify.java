/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A convenient interface to support object serialization to JSON. The default implementation is using
 * <code>com.google.gson.Gson</code>.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
public interface JsonStringify {
    /**
     * Serialize this object to JSON with indent.
     *
     * @return JSON string
     */
    default String toJson() {
        var gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
        return gson.toJson(this);
    }
}
