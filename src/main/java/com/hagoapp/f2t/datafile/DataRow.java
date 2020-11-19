/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.util.ArrayList;
import java.util.List;

public class DataRow {
    private int rowNo;

    private List<DataCell> cells = new ArrayList<>();

    public int getRowNo() {
        return rowNo;
    }

    public void setRowNo(int rowNo) {
        this.rowNo = rowNo;
    }

    public List<DataCell> getCells() {
        return cells;
    }
}
