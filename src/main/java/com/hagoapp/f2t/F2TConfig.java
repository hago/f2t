/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

public class F2TConfig {
    private String targetTable;
    private String targetSchema = "";
    private boolean addBatch = true;
    private boolean clearTable = false;
    private boolean createTableIfNeeded = true;
    private String batchColumnName = "f2tBatch";

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    public boolean isAddBatch() {
        return addBatch;
    }

    public void setAddBatch(boolean addBatch) {
        this.addBatch = addBatch;
    }

    public boolean isClearTable() {
        return clearTable;
    }

    public void setClearTable(boolean clearTable) {
        this.clearTable = clearTable;
    }

    public boolean isCreateTableIfNeeded() {
        return createTableIfNeeded;
    }

    public void setCreateTableIfNeeded(boolean createTableIfNeeded) {
        this.createTableIfNeeded = createTableIfNeeded;
    }

    public String getBatchColumnName() {
        return batchColumnName;
    }

    public void setBatchColumnName(String batchColumnName) {
        this.batchColumnName = batchColumnName;
    }

    @Override
    public String toString() {
        return "F2TConfig{" +
                "targetTable='" + targetTable + '\'' +
                ", targetSchema='" + targetSchema + '\'' +
                ", addBatch=" + addBatch +
                ", clearTable=" + clearTable +
                ", createTableIfNeeded=" + createTableIfNeeded +
                ", batchColumnName='" + batchColumnName + '\'' +
                '}';
    }
}
