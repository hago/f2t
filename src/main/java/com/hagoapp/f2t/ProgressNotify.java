/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.ParseResult;

/**
 * An interface to be watch the progress of running of a <code>D2TProcess</code> object.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
public interface ProgressNotify {
    /**
     * Get notified when processing starts.
     */
    void onStart();

    /**
     * Get notified when process completes with a result.
     *
     * @param result process result
     */
    void onComplete(ParseResult result);

    /**
     * Get notified when process percentage changed.
     *
     * @param progress ratio of process, presented as float between 0.0 to 1.0
     */
    void onProgress(float progress);
}
