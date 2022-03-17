/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.parquet;

import com.google.gson.Gson;
import com.hagoapp.f2t.F2TException;
import com.hagoapp.f2t.FileParser;
import com.hagoapp.f2t.csv.CsvTestConfig;
import com.hagoapp.f2t.datafile.DataTypeDeterminer;
import com.hagoapp.f2t.datafile.LeastTypeDeterminer;
import com.hagoapp.f2t.datafile.MostTypeDeterminer;
import com.hagoapp.f2t.datafile.parquet.ParquetWriter;
import com.hagoapp.f2t.datafile.parquet.ParquetWriterConfig;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ParquetFileTest {

    private static final Map<String, DataTypeDeterminer> testConfigFiles = Map.of(
            //"./tests/csv/shuihudata.json", new MostTypeDeterminer(),
            "./tests/csv/shuihudata_least.json", new LeastTypeDeterminer()
    );

    @Test
    public void testWriteParquet() throws IOException, F2TException {
        for (var item : testConfigFiles.entrySet()) {
            var testConfigFile = item.getKey();
            try (FileInputStream fis = new FileInputStream(testConfigFile)) {
                String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
                var testConfig = new Gson().fromJson(json, CsvTestConfig.class);
                var parser = new FileParser(testConfig.getFileInfo());
                var data = parser.extractData();
                var pwConfig = new ParquetWriterConfig("com.hagoapp.f2t", "shuihu", "x.parquet");
                new ParquetWriter(data, pwConfig).write();
            }
        }
    }
}
