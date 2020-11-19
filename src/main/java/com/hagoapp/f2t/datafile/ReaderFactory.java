/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

public class ReaderFactory {
    public static Reader getReader(FileInfo fileInfo) {
        FileType type = fileInfo.getType();
        switch (type) {
            case CSV:
            case Excel:
            case ExcelOpenXML:
            default:
                throw new UnsupportedOperationException(String.format("file type '%s' is not supported", type.name()));
        }
    }
}
