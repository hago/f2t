/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.excel

import com.google.gson.Gson
import com.hagoapp.f2t.FileParser
import com.hagoapp.f2t.FileTestObserver
import com.hagoapp.f2t.datafile.FileColumnTypeDeterminer
import com.hagoapp.f2t.datafile.FileTypeDeterminer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.charset.StandardCharsets

class TypedExcelReadTest {
    private val observer = FileTestObserver()

    companion object {
        private val testConfigFiles = mapOf(
            "./tests/excel/shuihudata_untyped.json" to FileColumnTypeDeterminer.MostTypeDeterminer,
            "./tests/excel/shuihudata_untyped_xls.json" to FileColumnTypeDeterminer.MostTypeDeterminer,
            "./tests/excel/shuihudata_untyped_least.json" to FileColumnTypeDeterminer.LeastTypeDeterminer,
            "./tests/excel/shuihudata_untyped_xls_least.json" to FileColumnTypeDeterminer.LeastTypeDeterminer
        )
        private lateinit var testConfigs: Map<ExcelTestConfig, FileColumnTypeDeterminer>

        @BeforeAll
        @JvmStatic
        @Throws(IOException::class)
        fun loadConfig() {
            testConfigs = testConfigFiles.map { (testConfigFile, determiner) ->
                FileInputStream(testConfigFile).use { fis ->
                    val json = String(fis.readAllBytes(), StandardCharsets.UTF_8)
                    val testConfig = Gson().fromJson(json, ExcelTestConfig::class.java)
                    val realExcel = File(
                        System.getProperty("user.dir"),
                        testConfig.fileInfo.filename!!
                    ).absolutePath
                    testConfig.fileInfo.filename = realExcel
                    Pair(testConfig, determiner)
                }
            }.toMap()
        }
    }

    @Test
    fun readExcel() {
        observer.isRowDetail = true
        testConfigs.forEach { (testConfig, determiner) ->
            println("test ${testConfig.fileInfo.filename} using $determiner")
            val parser = FileParser(testConfig.fileInfo)
            parser.determiner = FileTypeDeterminer(determiner)
            parser.addObserver(observer)
            parser.parse()
            Assertions.assertEquals(testConfig.expect.rowCount, observer.rowCount)
            Assertions.assertEquals(testConfig.expect.columnCount, observer.columns.size)
            Assertions.assertEquals(
                testConfig.expect.types,
                observer.columns.values.associate { (def, _) -> Pair(def.name, def.dataType) }
            )
        }
    }

    @Test
    fun extractExcel() {
        testConfigs.forEach { (testConfig, determiner) ->
            println("test ${testConfig.fileInfo.filename} using $determiner")
            val parser = FileParser(testConfig.fileInfo)
            parser.determiner = FileTypeDeterminer(determiner)
            parser.addObserver(observer)
            val table = parser.extractData()
            Assertions.assertEquals(testConfig.expect.rowCount, table.rows.size)
            Assertions.assertEquals(testConfig.expect.columnCount, table.columnDefinition.size)
            Assertions.assertEquals(
                testConfig.expect.types,
                table.columnDefinition.map { Pair(it.name, it.dataType) }.toMap()
            )
        }
    }
}
