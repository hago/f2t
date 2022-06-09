/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.excel

import org.apache.poi.ss.usermodel.WorkbookFactory
import java.io.FileInputStream
import java.io.InputStream

/**
 * This class is used to parse sheet information of an excel file, no any data reading will trigger.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class ExcelDataFileParser {

    private lateinit var info: ExcelInfo

    private fun init(input: InputStream) {
        WorkbookFactory.create(input).use { workbook ->
            info = ExcelInfo(
                sheets = (0 until workbook.numberOfSheets).map { i ->
                    val sheet = workbook.getSheetAt(i)
                    val row = sheet.getRow(0)
                    ExcelSheetInfo(
                        rowCount = sheet.lastRowNum - sheet.firstRowNum + 1,
                        columns = (row.firstCellNum until row.lastCellNum).mapNotNull { j ->
                            row.getCell(j)?.stringCellValue
                        },
                        name = sheet.sheetName
                    )
                }
            )
        }
    }

    private constructor()

    constructor(input: InputStream) : this() {
        init(input)
    }

    constructor(fileName: String) : this() {
        FileInputStream(fileName).use { init(it) }
    }

    /**
     * Return parsed meta information of the excel.
     *
     * @return excel metadata
     */
    fun getInfo(): ExcelInfo {
        return info
    }
}
