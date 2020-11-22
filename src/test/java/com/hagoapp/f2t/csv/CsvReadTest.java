package com.hagoapp.f2t.csv;

import com.google.gson.Gson;
import com.hagoapp.f2t.FileParser;
import com.hagoapp.f2t.ParseObserver;
import com.hagoapp.f2t.datafile.ColumnDefinition;
import com.hagoapp.f2t.datafile.DataRow;
import com.hagoapp.f2t.datafile.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

public class CsvReadTest {

    private static final String testConfigFile = "./tests/csv/shuihudata.json";
    private static CsvTestConfig testConfig;

    @BeforeAll
    public static void loadConfig() throws IOException {
        try (FileInputStream fis = new FileInputStream(testConfigFile)) {
            String json = new String(fis.readAllBytes());
            testConfig = new Gson().fromJson(json, CsvTestConfig.class);
            String realCsv = new File(System.getProperty("user.dir"),
                    testConfig.getFileInfo().getFilename()).getAbsolutePath();
            System.out.println(realCsv);
            testConfig.getFileInfo().setFilename(realCsv);
        }
    }

    @Test
    public void readCsv() {
        Assertions.assertDoesNotThrow(() -> {
            FileParser parser = new FileParser(testConfig.getFileInfo());
            parser.addWatcher(observer);
            parser.run();
        });
    }

    ParseObserver observer = new ParseObserver() {
        @Override
        public void onParseStart() {
            System.out.println("start");
        }

        @Override
        public void onColumnsParsed(@NotNull List<ColumnDefinition> columnDefinitionList) {
            System.out.println("column parsed");
        }

        @Override
        public void onColumnTypeDetermined(@NotNull List<ColumnDefinition> columnDefinitionList) {
            System.out.println("column definition determmined");
        }

        @Override
        public void onRowRead(@NotNull DataRow row) {
            System.out.println(String.format("row %d get", row.getRowNo()));
        }

        @Override
        public void onParseComplete(@NotNull ParseResult result) {
            System.out.println("complete");
        }

        @Override
        public boolean onRowError(@NotNull Throwable e) {
            System.out.println("error occurs");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    };
}
