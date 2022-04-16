package com.hagoapp.f2t.datafile.excel

/**
 * Meta information of a sheet in excel file.
 *
 * @property columns    list of column names
 * @property rowCount   count of rows in sheet
 * @property name   name of sheet, empty if not a named sheet
 * @author Chaojun Sun
 * @since 0.6
 */
data class ExcelSheetInfo(
    val columns: List<String>,
    val rowCount: Int,
    val name: String
)
