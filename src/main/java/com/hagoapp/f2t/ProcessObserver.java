/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

public interface ProcessObserver {
    void onTableExisted();
    void onTableCreated();
    void onRowInserted(int num);
    void onStart();
    void onComplete();
    void onDbError(Throwable e);
    void onFileError(Throwable e);
}
