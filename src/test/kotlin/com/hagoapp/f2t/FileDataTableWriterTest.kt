package com.hagoapp.f2t

import com.google.gson.Gson
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
                    val writer = FileDataTableWriter(con, f2TConfig, cols)
                    data.rows.forEach { row -> writer.writeRow(row) }
                    con.flushRows(tbl)
                    val size = con.queryTableSize(tbl)
                    assertEquals(data.rows.size, size.toInt())
                    con.dropTable(tbl)
                }
            }
        }
    }
}
