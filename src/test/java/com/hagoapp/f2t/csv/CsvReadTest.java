/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t.csv;

import com.google.gson.Gson;
import com.hagoapp.f2t.FileParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;

public class CsvReadTest {

    private static final String testConfigFile = "./tests/csv/shuihudata.json";
    private static CsvTestConfig testConfig;

    @BeforeAll
    public static void loadConfig() throws IOException {
        try (FileInputStream fis = new FileInputStream(testConfigFile)) {
            String json = new String(fis.readAllBytes(), StandardCharsets.UTF_8);
            testConfig = new Gson().fromJson(json, CsvTestConfig.class);
            String realCsv = new File(System.getProperty("user.dir"),
                    testConfig.getFileInfo().getFilename()).getAbsolutePath();
            System.out.println(realCsv);
            testConfig.getFileInfo().setFilename(realCsv);
            System.out.println(testConfig);
        }
    }

    @Test
    public void readCsv() {
        observer.setRowDetail(true);
        Assertions.assertDoesNotThrow(() -> {
            FileParser parser = new FileParser(testConfig.getFileInfo());
            parser.addWatcher(observer);
            parser.run();
            Assertions.assertEquals(observer.getRowCount(), testConfig.getExpect().getRowCount());
            Assertions.assertEquals(observer.getColumns().size(), testConfig.getExpect().getColumnCount());
            Assertions.assertEquals(observer.getColumns(), testConfig.getExpect().getTypes());
        });
    }

    TestObserver observer = new TestObserver();
}
