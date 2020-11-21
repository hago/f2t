package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.ColumnDefinition;
import com.hagoapp.f2t.datafile.DataRow;
import com.hagoapp.f2t.datafile.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CsvReadTest {

    @Test
    public void readCsv() {
        Assertions.assertDoesNotThrow(() -> {
            FileParser parser = new FileParser(null);
            parser.addWatcher(observer);
            parser.run();
        });
    }

    ParseObserver observer = new ParseObserver() {
        @Override
        public void onParseStart() {

        }

        @Override
        public void onColumnsParsed(@NotNull List<ColumnDefinition> columnDefinitionList) {

        }

        @Override
        public void onColumnTypeDetermined(@NotNull List<ColumnDefinition> columnDefinitionList) {

        }

        @Override
        public void onRowRead(@NotNull DataRow row) {

        }

        @Override
        public void onParseComplete(@NotNull ParseResult result) {

        }

        @Override
        public boolean onRowError(@NotNull Throwable e) {
            throw new RuntimeException(e);
        }
    };
}
