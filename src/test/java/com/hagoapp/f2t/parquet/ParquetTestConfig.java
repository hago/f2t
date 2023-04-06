package com.hagoapp.f2t.parquet;

import com.hagoapp.f2t.FileTestExpect;
import com.hagoapp.f2t.datafile.parquet.FileInfoParquet;

public class ParquetTestConfig {

    private FileInfoParquet fileInfo;
    private FileTestExpect expect;

    public FileInfoParquet getFileInfo() {
        return fileInfo;
    }

    public void setFileInfo(FileInfoParquet fileInfo) {
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
        return "ParquetTestConfig{" +
                "fileInfo=" + fileInfo +
                ", expect=" + expect +
                '}';
    }
}
