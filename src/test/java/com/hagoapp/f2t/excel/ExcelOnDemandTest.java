/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.excel;

import com.hagoapp.f2t.Constants;
import com.hagoapp.f2t.FileParser;
import com.hagoapp.f2t.FileTestObserver;
import com.hagoapp.f2t.datafile.LeastTypeDeterminer;
import com.hagoapp.f2t.datafile.excel.FileInfoExcel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;

@EnabledIfSystemProperty(named = Constants.ON_DEMAND_EXCEL_FILE, matches = ".*")
public class ExcelOnDemandTest {

    private static FileInfoExcel info;

    @BeforeAll
    private static void loadFile() {
        var f = System.getProperty(Constants.ON_DEMAND_EXCEL_FILE);
        info = new FileInfoExcel();
        info.setFilename(f);
        if (System.getProperties().contains(Constants.ON_DEMAND_EXCEL_SHEET_INDEX)) {
            info.setSheetIndex(Integer.getInteger(Constants.ON_DEMAND_EXCEL_SHEET_INDEX));
        } else if (System.getProperties().containsKey(Constants.ON_DEMAND_EXCEL_SHEET_NAME)) {
            info.setSheetName(System.getProperty(Constants.ON_DEMAND_EXCEL_SHEET_NAME));
        } else {
            info.setSheetIndex(0);
        }
    }

    private final FileTestObserver observer = new FileTestObserver();

    @Test
    public void readTest() throws IOException {
        observer.setRowDetail(true);
        var fp = new FileParser(info);
        fp.setDefaultDeterminer(new LeastTypeDeterminer());
        fp.addObserver(observer);
        fp.parse();
        Assertions.assertTrue(observer.getRowCount() > 0);
        System.out.println(observer.getColumns().values());
    }
}
