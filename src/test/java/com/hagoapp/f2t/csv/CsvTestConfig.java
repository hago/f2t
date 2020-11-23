package com.hagoapp.f2t.csv;

import com.hagoapp.f2t.Expect;
import com.hagoapp.f2t.datafile.csv.FileInfoCsv;

public class CsvTestConfig {

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
