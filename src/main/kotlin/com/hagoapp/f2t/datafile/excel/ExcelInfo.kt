package com.hagoapp.f2t.datafile.excel

/**
 * Meta data of an excel file.
 *
 * @property sheets list of sheet meta information
 * @author Chaojun Sun
 * @since 0.6
 */
data class ExcelInfo(
    val sheets: List<ExcelSheetInfo>
)
