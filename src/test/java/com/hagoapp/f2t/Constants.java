/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import java.util.Map;

public class Constants {
    public static final String DATABASE_CONFIG_FILE = "f2t.db";
    public static final String DATABASE_TEST_SCHEMA = "f2t.db.schema";
    public static final String DATABASE_TEST_TABLE = "f2t.db.table";

    public static final String PROCESS_CONFIG_FILE = "f2t.process";
    public static final String FILE_CONFIG_FILE = "f2t.file";

    public static final Map<String, String> configDescriptions = Map.of(
            DATABASE_CONFIG_FILE, "config file of target database",
            PROCESS_CONFIG_FILE, "config file of f2t process",
            FILE_CONFIG_FILE, "config file of file to table process"
    );

    public static final String ON_DEMAND_EXCEL_FILE = "f2t.excel";
    public static final String ON_DEMAND_EXCEL_SHEET_INDEX = "f2t.excel.sheet.index";
    public static final String ON_DEMAND_EXCEL_SHEET_NAME = "f2t.excel.sheet.name";

    public static final String ON_DEMAND_CSV_FILE = "f2t.csv";
    public static final String ON_DEMAND_CSV_FILE_ENCODING = "f2t.csv.encoding";
    public static final String ON_DEMAND_CSV_FILE_DELIMITER = "f2t.csv.delimit";
    public static final String ON_DEMAND_CSV_FILE_QUOTE = "f2t.csv.quote";

    public static final String KEEP_PARQUET_FILE_GENERATED = "f2t.parquet.clean";
}
