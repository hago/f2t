package com.hagoapp.f2t.csv;

import com.hagoapp.f2t.FileTestExpect;
import com.hagoapp.f2t.datafile.csv.FileInfoCsv;

public class CsvTestConfig {

    private FileInfoCsv fileInfo;
    private FileTestExpect expect;

    public FileInfoCsv getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfoCsv fileInfo) {
        this.fileInfo = fileInfo;
    }

    public FileTestExpect getExpect() {
        return expect;
    }

    public void setExpect(FileTestExpect expect) {
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
