/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.excel

import com.google.gson.Gson
import com.hagoapp.f2t.FileParser
import com.hagoapp.f2t.FileTestObserver
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.charset.StandardCharsets

class ExcelReadTest {
    private val observer = FileTestObserver()

    companion object {
        private const val testConfigFile = "./tests/excel/shuihudata_untyped.json"
        private lateinit var testConfig: ExcelTestConfig

        @BeforeAll
        @JvmStatic
        @Throws(IOException::class)
        fun loadConfig() {
            FileInputStream(testConfigFile).use { fis ->
                val json = String(fis.readAllBytes(), StandardCharsets.UTF_8)
                testConfig = Gson().fromJson(json, ExcelTestConfig::class.java)
                println(testConfig)
                val realCsv = File(
                    System.getProperty("user.dir"),
                    testConfig.fileInfo.filename
                ).absolutePath
                println(realCsv)
                testConfig.fileInfo.filename = realCsv
                println(testConfig)
            }
        }
    }

    @Test
    fun readExcelUnTyped() {
        observer.isRowDetail = true
        Assertions.assertDoesNotThrow {
            val parser = FileParser(testConfig.fileInfo)
            parser.addWatcher(observer)
            parser.run()
            Assertions.assertEquals(
                observer.rowCount,
                testConfig.expect.rowCount
            )
            Assertions.assertEquals(
                observer.columns.size,
                testConfig.expect.columnCount
            )
            Assertions.assertEquals(
                observer.columns,
                testConfig.expect.types
            )
        }
    }
}
