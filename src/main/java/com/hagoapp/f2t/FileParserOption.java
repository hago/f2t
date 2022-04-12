package com.hagoapp.f2t;

/**
 * The options for <code>FileParser</code> to start parsing.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
public class FileParserOption {
    private boolean readData = true;
    private boolean inferColumnTypes = true;

    /**
     * Whether to skip data lines reading.
     *
     * @return true if data will be read, otherwise false
     */
    public boolean isReadData() {
        return readData;
    }

    /**
     * Set whether to skip data line reading.
     *
     * @param readData true if data will be read, otherwise false
     */
    public void setReadData(boolean readData) {
        if (readData) {
            this.inferColumnTypes = true;
        }
        this.readData = readData;
    }

    /**
     * Whether to infer data types of columns.
     *
     * @return true if data type will be inferred, otherwise false
     */
    public boolean isInferColumnTypes() {
        return inferColumnTypes;
    }

    /**
     * set whether to infer data types of columns.
     *
     * @param inferColumnTypes true if data type will be inferred, otherwise false
     */
    public void setInferColumnTypes(boolean inferColumnTypes) {
        if (!inferColumnTypes) {
            this.readData = false;
        }
        this.inferColumnTypes = inferColumnTypes;
    }
}
