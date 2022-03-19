/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.csv;

import com.hagoapp.f2t.Constants;
import com.hagoapp.f2t.FileParser;
import com.hagoapp.f2t.FileTestObserver;
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer;
import com.hagoapp.f2t.datafile.FileTypeDeterminer;
import com.hagoapp.f2t.datafile.csv.FileInfoCsv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;

@EnabledIfSystemProperty(named = Constants.ON_DEMAND_CSV_FILE, matches = ".*")
public class CsvOnDemandTest {
    private static FileInfoCsv info;

    @BeforeAll
    private static void loadFile() {
        var f = System.getProperty(Constants.ON_DEMAND_CSV_FILE);
        info = new FileInfoCsv();
        info.setFilename(f);
        if (System.getProperties().contains(Constants.ON_DEMAND_CSV_FILE_DELIMITER)) {
            info.setDelimiter(System.getProperty(Constants.ON_DEMAND_CSV_FILE_DELIMITER).charAt(0));
        }
        if (System.getProperties().containsKey(Constants.ON_DEMAND_CSV_FILE_QUOTE)) {
            info.setQuote(System.getProperty(Constants.ON_DEMAND_EXCEL_SHEET_NAME).charAt(0));
        }
        if (System.getProperties().containsKey(Constants.ON_DEMAND_CSV_FILE_ENCODING)) {
            info.setEncoding(System.getProperty(Constants.ON_DEMAND_CSV_FILE_ENCODING));
        }
    }

    private final FileTestObserver observer = new FileTestObserver();

    @Test
    public void readTest() throws IOException {
        observer.setRowDetail(true);
        var fp = new FileParser(info);
        fp.setDeterminer(new FileTypeDeterminer(FileColumnTypeDeterminer.Companion.getLeastTypeDeterminer()));
        fp.addObserver(observer);
        fp.parse();
        Assertions.assertTrue(observer.getRowCount() > 0);
        System.out.println(observer.getColumns().values());
    }
}
