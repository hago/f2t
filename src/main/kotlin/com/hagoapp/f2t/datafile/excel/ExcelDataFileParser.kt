package com.hagoapp.f2t.datafile.excel

import org.apache.poi.ss.usermodel.WorkbookFactory
import java.io.FileInputStream

class ExcelDataFileParser(fileName: String) {

    private val info: ExcelInfo

    init {
        FileInputStream(fileName).use { fi ->
            WorkbookFactory.create(fi).use { workbook ->
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
    }

    fun getInfo(): ExcelInfo {
        return info
    }
}
