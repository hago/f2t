package com.hagoapp.f2t

import com.google.gson.Gson
import com.hagoapp.f2t.database.DbConnection
import com.hagoapp.f2t.database.DbConnectionFactory
import com.hagoapp.f2t.database.TableName
import com.hagoapp.f2t.database.config.DbConfigReader
import com.hagoapp.f2t.datafile.FileInfo
import com.hagoapp.f2t.datafile.FileInfoReader
import com.hagoapp.f2t.datafile.ReaderFactory
import com.nimbusds.jose.util.StandardCharset
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.io.FileInputStream

class FileDataTableWriterTest {

    private val f2tConfigFiles = listOf(
        "tests/process/f2t-batch-new-clear.json",
        "tests/process/f2t-nobatch-new.json",
    )

    private val dbConfigFile = "tests/process/pgsql.sample.json"
    private val srcConfigFile = "tests/process/shuihucsv.json"
    private val logger = LoggerFactory.getLogger(FileDataTableWriterTest::class.java)

    private fun loadF2tConfigs(): List<F2TConfig> {
        val gson = Gson()
        return f2tConfigFiles.map { fn ->
            FileInputStream(fn).use { fis ->
                val s = fis.readAllBytes().toString(StandardCharset.UTF_8)
                gson.fromJson(s, F2TConfig::class.java)
            }
        }
    }

    private fun loadSrcFileConfig(): FileInfo {
        return FileInfoReader.createFileInfo(srcConfigFile)
    }

    @Test
    fun testFileDataTableWriter() {
        val cols: List<FileColumnDefinition>
        val data: DataTable<FileColumnDefinition>
        val info = loadSrcFileConfig()
        ReaderFactory.getReader(info).use { reader ->
            reader.open(info)
            cols = reader.findColumns()
            val rows = mutableListOf<DataRow>()
            while (reader.hasNext()) {
                rows.add(reader.next())
            }
            data = DataTable(cols, rows)
        }
        DbConfigReader.readConfig(dbConfigFile).createConnection().use { conn ->
            DbConnectionFactory.createDbConnection(conn).use { con ->
                loadF2tConfigs().forEach { f2TConfig ->
                    val tbl = TableName(f2TConfig.targetTable, f2TConfig.targetSchema ?: "")
                    con.dropTable(tbl)
                    logger.debug("run config: {}", f2TConfig)
                    doWrite(con, f2TConfig, cols, data, tbl)
                    val size = con.queryTableSize(tbl).toInt()
                    assertEquals(data.rows.size, size)
                    f2TConfig.clearTable = false
                    doWrite(con, f2TConfig, cols, data, tbl)
                    assertTrue(size * 2 == con.queryTableSize(tbl).toInt())
                    f2TConfig.clearTable = true
                    doWrite(con, f2TConfig, cols, data, tbl)
                    assertTrue(size == con.queryTableSize(tbl).toInt())
                    con.dropTable(tbl)
                }
            }
        }
    }

    private fun doWrite(
        con: DbConnection, f2TConfig: F2TConfig, cols: List<FileColumnDefinition>,
        data: DataTable<FileColumnDefinition>, tbl: TableName
    ) {
        val writer = FileDataTableWriter(con, f2TConfig, cols)
        data.rows.forEach { row -> writer.writeRow(row) }
        con.flushRows(tbl)
    }
}
