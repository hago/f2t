package com.hagoapp.f2t.process

import com.google.gson.Gson
import com.hagoapp.f2t.*
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

@EnabledIfSystemProperties(
    EnabledIfSystemProperty(named = "f2t.db", matches = ".+"),
    EnabledIfSystemProperty(named = "f2t.file", matches = ".+"),
    EnabledIfSystemProperty(named = "f2t.process", matches = ".+")
)
class D2TProcessTester {

    private val dbConfigFile: String
    private val processConfigFile: String
    private val fileConfigFile: String
    private val logger = LoggerFactory.getLogger(D2TProcessTester::class.java)

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
    fun run() {
        val fileInfo = FileInfoReader.createFileInfo(fileConfigFile)
        fileInfo.filename = File(System.getProperty("user.dir"), fileInfo.filename!!).absolutePath
        val dbConfig = DbConfigReader.readConfig(dbConfigFile)
        val f2tConfig = Gson().fromJson(Files.readString(Path.of(processConfigFile)), F2TConfig::class.java)
        val parser = FileParser(fileInfo)
        val dtObserver = DataTableParserObserver()
        parser.addObserver(dtObserver)
        parser.parse()
        Assertions.assertTrue(dtObserver.succeeded)

        dbConfig.createConnection().use { con ->
            val process = D2TProcess(dtObserver.dataTableInfo(), con, f2tConfig)
            val result = process.run()
            logger.debug("D2T result: {}", result)
            Assertions.assertTrue(result.isSucceeded)
        }
    }

    override fun toString(): String {
        return "D2TProcessTester(dbConfigFile='$dbConfigFile', processConfigFile='$processConfigFile', fileConfigFile='$fileConfigFile')"
    }
}