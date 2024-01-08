/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.process

import com.google.gson.Gson
import com.hagoapp.f2t.*
import com.hagoapp.f2t.database.DbConnectionFactory
import com.hagoapp.f2t.database.TableName
import com.hagoapp.f2t.database.config.DbConfigReader
import com.hagoapp.f2t.datafile.FileInfoReader
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperties
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files
import java.nio.file.Path

class ProcessTester {

    private val dbConfigFile: String
    private val processConfigFile: String
    private val fileConfigFile: String
    private val logger = LoggerFactory.getLogger(ProcessTester::class.java)

    init {
        val required = listOf(Constants.DATABASE_CONFIG_FILE, Constants.PROCESS_CONFIG_FILE, Constants.FILE_CONFIG_FILE)
        if (required.any { System.getProperty(it) == null }) {
            println("Some configuration needs to be specified:")
            required.filter { System.getProperty(it) == null }
                .forEach { println("-D$it - ${Constants.configDescriptions[it]}") }
            //throw F2TException("config missing")
        }
        dbConfigFile = System.getProperty(Constants.DATABASE_CONFIG_FILE) ?: ""
        processConfigFile = System.getProperty(Constants.PROCESS_CONFIG_FILE) ?: ""
        fileConfigFile = System.getProperty(Constants.FILE_CONFIG_FILE) ?: ""
    }

    @Test
    @EnabledIfSystemProperties(
        EnabledIfSystemProperty(named = Constants.PROCESS_CONFIG_FILE, matches = ".+"),
        EnabledIfSystemProperty(named = Constants.DATABASE_CONFIG_FILE, matches = ".+"),
        EnabledIfSystemProperty(named = Constants.FILE_CONFIG_FILE, matches = ".+")
    )
    fun testCustomCase() {
        val result = runCase(dbConfigFile, processConfigFile, fileConfigFile)
        Assertions.assertTrue(result.succeeded())
    }

    private fun runCase(dbCfgFile: String, processCfgFile: String, srcCfgFile: String): F2TResult {
        val fileInfo = FileInfoReader.createFileInfo(srcCfgFile)
        fileInfo.filename = File(System.getProperty("user.dir"), fileInfo.filename!!).absolutePath
        val dbConfig = DbConfigReader.readConfig(dbCfgFile)
        val f2tConfig = Gson().fromJson(Files.readString(Path.of(processCfgFile)), F2TConfig::class.java)
        val parser = FileParser(fileInfo)
        val observer = FileTestObserver()
        parser.addObserver(observer)
        dbConfig.createConnection().use { con ->
            val targetTable = TableName(f2tConfig.targetTable, f2tConfig.targetSchema ?: "")
            DbConnectionFactory.createDbConnection(con).use { it.dropTable(targetTable) }
            val process = F2TProcess(parser, con, f2tConfig)
            process.run()
            logger.debug("result: {}", process.result)
            val fileRowCount = observer.rowCount
            val fileCols = observer.columns.keys
            DbConnectionFactory.createDbConnection(con).use {
                val rowCount = it.queryTableSize(targetTable)
                val cols = it.getExistingTableDefinition(targetTable).columns.map { col -> col as ColumnDefinition }
                val colMatcher = if (it.isCaseSensitive())
                    { a: String, b: String -> a.compareTo(b, false) == 0 }
                else { a: String, b: String -> a.compareTo(b, true) == 0 }
                it.dropTable(targetTable)
                Assertions.assertEquals(fileRowCount, rowCount)
                Assertions.assertEquals(if (!f2tConfig.isAddBatch) fileCols.size else (fileCols.size + 1), cols.size)
                Assertions.assertTrue(fileCols.all { fileCol ->
                    cols.any { dbCol ->
                        colMatcher.invoke(fileCol, dbCol.name)
                    }
                })
            }
            return process.result
        }
    }

    override fun toString(): String {
        return "ProcessTester(dbConfigFile='$dbConfigFile', processConfigFile='$processConfigFile', fileConfigFile='$fileConfigFile')"
    }

    @Test
    fun testDefaultCase() {
        val defaultDbConfigFile = "tests/process/pgsql.sample.json"
        val defaultProcessConfigFiles = listOf(
            "tests/process/f2t-batch-new-clear.json",
            "tests/process/f2t-nobatch-new.json"
        )
        val defaultFileConfigFile = "tests/process/shuihucsv.json"
        for (process in defaultProcessConfigFiles) {
            val result = runCase(defaultDbConfigFile, process, defaultFileConfigFile)
            Assertions.assertTrue(result.succeeded())
        }
    }
}
