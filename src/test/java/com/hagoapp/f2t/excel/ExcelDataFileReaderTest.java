/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.excel;

import com.hagoapp.f2t.datafile.FileInfoReader;
import com.hagoapp.f2t.datafile.excel.ExcelDataFileReader;
import com.hagoapp.f2t.datafile.excel.FileInfoExcel;
import com.hagoapp.f2t.datafile.excel.FileInfoExcelX;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class ExcelDataFileReaderTest {

    private static final String EMPTY_EXCEL = "tests/excel/empty.xlsx";
    private static final List<String> TEST_EXCEL_FILES = List.of(
            "tests/excel/shuihudata.xls",
            "tests/excel/shuihudata.xlsx"
    );

    private final Logger logger = LoggerFactory.getLogger(ExcelDataFileReaderTest.class);

    @Test
    void testOpenEmptyExcel() {
        var info = new FileInfoExcel();
        info.setSheetIndex(0);
        info.setFilename(EMPTY_EXCEL);
        try (var reader = new ExcelDataFileReader()) {
            reader.open(info);
            var columns = reader.inferColumnTypes(-1);
            Assertions.assertTrue(columns.isEmpty());
            var columns1 = reader.findColumns();
            Assertions.assertTrue(columns1.isEmpty());
        }
    }

    @Test
    void testOpenExcel() {
        for (var excel : TEST_EXCEL_FILES) {
            logger.debug("test Excel reading with {}", excel);
            var info = excel.endsWith("xlsx") ? new FileInfoExcelX() : new FileInfoExcel();
            info.setSheetIndex(0);
            info.setFilename(excel);
            try (var reader = new ExcelDataFileReader()) {
                reader.open(info);
                var columns = reader.inferColumnTypes(-1);
                Assertions.assertFalse(columns.isEmpty());
                var columns1 = reader.findColumns();
                Assertions.assertFalse(columns1.isEmpty());
                logger.debug("columns: {}", columns1);
            }
        }
    }

}
