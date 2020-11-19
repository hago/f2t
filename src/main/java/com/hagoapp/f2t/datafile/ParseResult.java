/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class ParseResult {
    private final long startTime = Instant.now().toEpochMilli();
    private Long endTime;
    private final Map<Long, Throwable> errors = new HashMap<>();

    public long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public Map<Long, Throwable> getErrors() {
        return errors;
    }

    public void end() {
        endTime = Instant.now().toEpochMilli();
    }

    public Long milliSecondsUsed() {
        return endTime == null ? null : endTime - startTime;
    }

    public boolean isSucceeded() {
        return errors.isEmpty();
    }

    public void addError(long rowNo, Throwable e) {
        errors.put(rowNo, e);
    }
}
