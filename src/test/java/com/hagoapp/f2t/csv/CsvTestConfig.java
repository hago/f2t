package com.hagoapp.f2t.csv;

import com.hagoapp.f2t.datafile.csv.FileInfoCsv;

import java.sql.JDBCType;
import java.util.Map;

public class CsvTestConfig {
    public class Expect {
        private int rowCount;
        private int columnCount;
        private Map<String, JDBCType> types;

        public Map<String, JDBCType> getTypes() {
            return types;
        }

        public void setTypes(Map<String, JDBCType> types) {
            this.types = types;
        }

        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
        }

        public int getColumnCount() {
            return columnCount;
        }

        public void setColumnCount(int columnCount) {
            this.columnCount = columnCount;
        }

        @Override
        public String toString() {
            return "Expect{" +
                    "rowCount=" + rowCount +
                    ", columnCount=" + columnCount +
                    ", types=" + types +
                    '}';
        }
    }

    private FileInfoCsv fileInfo;
    private Expect expect;

    public FileInfoCsv getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfoCsv fileInfo) {
        this.fileInfo = fileInfo;
    }

    public Expect getExpect() {
        return expect;
    }

    public void setExpect(Expect expect) {
        this.expect = expect;
    }

    @Override
    public String toString() {
        return "CsvTestConfig{" +
                "fileInfo=" + fileInfo +
                ", expect=" + expect +
                '}';
    }
}
