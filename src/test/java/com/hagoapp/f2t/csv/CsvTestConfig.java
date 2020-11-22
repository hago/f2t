package com.hagoapp.f2t.csv;

import com.hagoapp.f2t.datafile.csv.FileInfoCsv;

import java.sql.JDBCType;
import java.util.List;

public class CsvTestConfig {
    public class Expect {
        private int rowCount;
        private int columnCount;
        private List<JDBCType> types;

        public List<JDBCType> getTypes() {
            return types;
        }

        public void setTypes(List<JDBCType> types) {
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
}
