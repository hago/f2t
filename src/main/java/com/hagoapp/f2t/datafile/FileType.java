/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

public enum FileType {
    Unknown,
    CSV,
    Excel,
    ExcelOpenXML;

    public static FileType getFromExtension(String ext) {
        switch (ext.trim().toLowerCase()) {
            case "csv":
                return CSV;
            case "xls":
                return Excel;
            case "xlsx":
                return ExcelOpenXML;
            default:
                return Unknown;
        }
    }
}
