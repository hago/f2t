package com.hagoapp.f2t;

public class FileParserOption {
    private boolean readData = true;
    private boolean inferColumnTypes = true;

    public boolean isReadData() {
        return readData;
    }

    public void setReadData(boolean readData) {
        if (readData) {
            this.inferColumnTypes = true;
        }
        this.readData = readData;
    }

    public boolean isInferColumnTypes() {
        return inferColumnTypes;
    }

    public void setInferColumnTypes(boolean inferColumnTypes) {
        if (!inferColumnTypes) {
            this.readData = false;
        }
        this.inferColumnTypes = inferColumnTypes;
    }
}
