/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The logger used by F2T library, which is basically an instance of <Code>org.slf4j.Logger</Code>.
 *
 * @author Chaojun Sun
 * @since 0.1
 */
public class F2TLogger {
    /**
     * Get the logger instance.
     *
     * @return logger
     */
    public static Logger getLogger() {
        return LoggerFactory.getLogger(F2TLogger.class.getPackageName());
    }
}
