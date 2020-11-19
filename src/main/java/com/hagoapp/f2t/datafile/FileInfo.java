/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.io.File;

public class FileInfo {
    protected String filename;

    public FileInfo(String name) {
        filename = name;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public FileType getType() {
        int i = filename.lastIndexOf('.');
        if (i < 0) {
            return FileType.Unknown;
        }
        switch (filename.substring(i).toLowerCase()) {
            case ".csv":
                return FileType.CSV;
            case ".xls":
                return FileType.Excel;
            case ".xlsx":
                return FileType.ExcelOpenXML;
            default:
                return FileType.Unknown;
        }
    }
}
