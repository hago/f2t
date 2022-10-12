/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.excel

import com.google.gson.Gson
import com.hagoapp.f2t.datafile.excel.ExcelDataFileParser
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileInputStream

class ExcelParserTest {

    private val testConfigFiles = listOf(
        "./tests/excel/shuihudata_untyped.json",
        "./tests/excel/shuihudata_untyped_xls.json",
        "./tests/excel/shuihudata_untyped_least.json",
        "./tests/excel/shuihudata_untyped_xls_least.json"
    )

    private fun readConfigFile(fileName: String): ExcelTestConfig {
        FileInputStream(fileName).use {
            val json = String(it.readAllBytes())
            return Gson().fromJson(json, ExcelTestConfig::class.java)
        }
    }

    @Test
    fun testParseFileStream() {
        for (configFile in testConfigFiles) {
            val config = readConfigFile(configFile)
            FileInputStream(config.fileInfo.filename!!).use {
                val parser = ExcelDataFileParser(it)
                Assertions.assertEquals(1, parser.getInfo().sheets.size)
                Assertions.assertEquals(config.expect.rowCount, parser.getInfo().sheets[0].rowCount - 1)
                Assertions.assertEquals(config.expect.columnCount, parser.getInfo().sheets[0].columns.size)
                Assertions.assertTrue(
                    config.expect.types.keys.subtract(parser.getInfo().sheets[0].columns.toSet()).isEmpty()
                )
            }
        }
    }

    @Test
    fun testParseFileName() {
        for (configFile in testConfigFiles) {
            val config = readConfigFile(configFile)
            val parser = ExcelDataFileParser(config.fileInfo.filename!!)
            Assertions.assertEquals(1, parser.getInfo().sheets.size)
            Assertions.assertEquals(config.expect.rowCount, parser.getInfo().sheets[0].rowCount - 1)
            Assertions.assertEquals(config.expect.columnCount, parser.getInfo().sheets[0].columns.size)
            Assertions.assertTrue(
                config.expect.types.keys.subtract(parser.getInfo().sheets[0].columns.toSet()).isEmpty()
            )
        }
    }

    @Test
    fun testEmptyXlsx() {
        val fn = File("./tests/excel/empty.xlsx")
        FileInputStream(fn).use { fis ->
            val p = ExcelDataFileParser(fis)
            val info = p.getInfo()
            Assertions.assertEquals(3, info.sheets.size)
            Assertions.assertTrue(info.sheets[0].columns.isEmpty())
            Assertions.assertFalse(info.sheets[1].columns.isEmpty())
            Assertions.assertTrue(info.sheets[2].columns.isEmpty())
        }
    }
}
