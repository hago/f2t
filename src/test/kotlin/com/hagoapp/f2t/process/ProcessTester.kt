/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.process

import com.google.gson.Gson
import com.hagoapp.f2t.*
import com.hagoapp.f2t.database.DbConnectionFactory
import com.hagoapp.f2t.database.config.DbConfigReader
import com.hagoapp.f2t.datafile.FileInfoReader
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import java.nio.file.Path

class ProcessTester {

    private val dbConfigFile: String
    private val processConfigFile: String
    private val fileConfigFile: String
    private val logger = F2TLogger.getLogger()

    init {
        val required = listOf(Constants.DATABASE_CONFIG_FILE, Constants.PROCESS_CONFIG_FILE, Constants.FILE_CONFIG_FILE)
        if (required.any { System.getProperty(it) == null }) {
            println("Some configuration needs to be specified:")
            required.filter { System.getProperty(it) == null }
                .forEach { println("-D$it - ${Constants.configDescriptions[it]}") }
            throw F2TException("config missing")
        }
        dbConfigFile = System.getProperty(Constants.DATABASE_CONFIG_FILE)
        processConfigFile = System.getProperty(Constants.PROCESS_CONFIG_FILE)
        fileConfigFile = System.getProperty(Constants.FILE_CONFIG_FILE)
    }

    @Test
    fun run() {
        val fileInfo = FileInfoReader.createFileInfo(fileConfigFile)
        fileInfo.filename = File(System.getProperty("user.dir"), fileInfo.filename).absolutePath
        val dbConfig = DbConfigReader.readConfig(dbConfigFile)
        DbConnectionFactory.createDbConnection(dbConfig).use { dbConnection ->
            val f2tConfig = Gson().fromJson(Files.readString(Path.of(processConfigFile)), F2TConfig::class.java)
            val parser = FileParser(fileInfo)
            parser.addWatcher(FileTestObserver())
            val process = F2TProcess(parser, dbConnection, f2tConfig)
            process.run()
        }
    }

    override fun toString(): String {
        return "ProcessTester(dbConfigFile='$dbConfigFile', processConfigFile='$processConfigFile', fileConfigFile='$fileConfigFile')"
    }
}
