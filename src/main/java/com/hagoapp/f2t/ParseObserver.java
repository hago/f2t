/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.ColumnDefinition;
import com.hagoapp.f2t.datafile.DataRow;
import com.hagoapp.f2t.datafile.ParseResult;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface ParseObserver {

    default void onParseStart() {
    }

    default void onColumnsParsed(@NotNull List<ColumnDefinition> columnDefinitionList) {
    }

    default void onColumnTypeDetermined(@NotNull List<ColumnDefinition> columnDefinitionList) {
    }

    default void onRowRead(@NotNull DataRow row) {
    }

    default void onParseComplete(@NotNull ParseResult result) {
    }

    default boolean onRowError(@NotNull Throwable e) {
        return true;
    }

    default void onRowCountDetermined(long rowCount) {
    }
}