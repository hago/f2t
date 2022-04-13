/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

/**
 * The configuration conating all necessary information required by a complete file to table process.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
public class F2TConfig {
    protected String targetTable;
    protected String targetSchema = "";
    protected boolean addBatch = true;
    protected boolean clearTable = false;
    protected boolean createTableIfNeeded = true;
    protected String batchColumnName = "f2tBatch";

    /**
     * The table name as target.
     *
     * @return table name
     */
    public String getTargetTable() {
        return targetTable;
    }

    /**
     * Set the target table name,
     *
     * @param targetTable table name
     */
    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    /**
     * The schema name in target database.
     *
     * @return schema name
     */
    public String getTargetSchema() {
        return targetSchema;
    }

    /**
     * Set the schema name in target database.
     *
     * @param targetSchema schema name
     */
    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    /**
     * Whether to add a batch number column to identify from different running.
     *
     * @return true to add a batch number column, otherwise false
     */
    public boolean isAddBatch() {
        return addBatch;
    }

    /**
     * Set whether to add a batch number column to identify from different running.
     *
     * @param addBatch true to add a batch number column, otherwise false
     */
    public void setAddBatch(boolean addBatch) {
        this.addBatch = addBatch;
    }

    /**
     * Whether to clear existing data from target table.
     *
     * @return true to clear data, otherwise false
     */
    public boolean isClearTable() {
        return clearTable;
    }

    /**
     * Set whether to clear existing data from target table.
     *
     * @param clearTable true to clear data, otherwise false
     */
    public void setClearTable(boolean clearTable) {
        this.clearTable = clearTable;
    }

    /**
     * Whether to create table automatically if target table is not existed. If table exists, creation will be ignored.
     *
     * @return true to create, otherwise false
     */
    public boolean isCreateTableIfNeeded() {
        return createTableIfNeeded;
    }

    /**
     * Set whether to create table automatically if target table is not existed.
     *
     * @param createTableIfNeeded true to create, otherwise false
     */
    public void setCreateTableIfNeeded(boolean createTableIfNeeded) {
        this.createTableIfNeeded = createTableIfNeeded;
    }

    /**
     * Get the name of batch number column, it is "f2tBatch" by default.
     *
     * @return the name of batch number column
     */
    public String getBatchColumnName() {
        return batchColumnName;
    }

    /**
     * Set the name of batch number column.
     *
     * @param batchColumnName the name of batch number column
     */
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
