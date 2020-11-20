/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

/**
 * The custom exception for f2t. It's meant to used for errors thrown by f2t itself.
 *
 * @author Chaojun Sun
 * @since 1.0
 */
public class F2TException extends Exception {
    public F2TException(String message) {
        super(message);
    }

    public F2TException(String message, Throwable cause) {
        super(message, cause);
    }
}
