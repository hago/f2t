/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

public interface Reader extends Closeable, Iterator<DataRow> {
    void open(FileInfo fileInfo);

    List<ColumnDefinition> findColumns();

    default List<ColumnDefinition> inferColumnTypes() {
        return inferColumnTypes(-1);
    }

    List<ColumnDefinition> inferColumnTypes(long sampleRowCount);
}
