/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util;

import com.hagoapp.f2t.F2TLogger;
import com.hagoapp.f2t.Quartet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DateTimeUtilsTest {

    private final Logger logger = LoggerFactory.getLogger(F2TLogger.class.getPackageName());

    /**
     * 1 - toDateTime
     * 2 - toDate
     * 3 - toTime
     */
    private final List<Quartet<String, Boolean, Boolean, Boolean>> cases = List.of(
            new Quartet<>("2021-04-01", false, true, false),
            new Quartet<>("23:12:24", false, false, true),
            new Quartet<>("2021-04-01 23:12:24", true, false, false),
            new Quartet<>("2021-04-01T23:12:24+08", true, false, false),
            new Quartet<>("2021-04-01T23:12:24Z", true, false, false)
    );

    @Test
    public void testDateTimeParsing() {
        for (var x : cases) {
            var dateTimeStr = x.getFirst();
            logger.debug("try parsing {}", dateTimeStr);
            var dt = DateTimeTypeUtils.stringToDateTimeOrNull(dateTimeStr);
            logger.debug("datetime: {}", dt);
            Assertions.assertEquals(x.getSecond(), dt != null);
            var d = DateTimeTypeUtils.stringToDateOrNull(dateTimeStr);
            logger.debug("date: {}", d);
            Assertions.assertEquals(x.getThird(), d != null);
            var t = DateTimeTypeUtils.stringToTimeOrNull(dateTimeStr);
            logger.debug("time: {}", t);
            Assertions.assertEquals(x.getFourth(), t != null);
        }
    }
}
