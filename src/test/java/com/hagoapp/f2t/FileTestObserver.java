/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.ColumnDefinition;
import com.hagoapp.f2t.datafile.DataRow;
import com.hagoapp.f2t.datafile.FileInfo;
import com.hagoapp.f2t.datafile.ParseResult;
import org.jetbrains.annotations.NotNull;

import java.sql.JDBCType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileTestObserver implements ParseObserver {
    private int rowCount;
    private Map<String, JDBCType> columns;
    private boolean rowDetail = false;

    public int getRowCount() {
        return rowCount;
    }

    public Map<String, JDBCType> getColumns() {
        return columns;
    }

    public boolean isRowDetail() {
        return rowDetail;
    }

    public void setRowDetail(boolean rowDetail) {
        this.rowDetail = rowDetail;
    }

    @Override
    public void onParseStart(@NotNull FileInfo fileInfo) {
        System.out.printf("start %s%n%s", fileInfo.getFilename(), System.getProperty("line.separator"));
    }

    @Override
    public void onColumnsParsed(@NotNull List<ColumnDefinition> columnDefinitionList) {
        System.out.println("column parsed");
        columns = columnDefinitionList.stream().collect(Collectors.toMap(
                ColumnDefinition::getName,
                col -> JDBCType.NULL
        ));
    }

    @Override
    public void onColumnTypeDetermined(@NotNull List<ColumnDefinition> columnDefinitionList) {
        System.out.println("column definition determined");
        columns = columnDefinitionList.stream().collect(Collectors.toMap(
                ColumnDefinition::getName,
                col -> col.getInferredType() != null ? col.getInferredType() : JDBCType.NULL
        ));
    }

    @Override
    public void onRowRead(@NotNull DataRow row) {
        System.out.printf("row %d get%n", row.getRowNo());
        if (rowDetail) {
            System.out.println(row);
        }
    }

    @Override
    public void onParseComplete(@NotNull FileInfo fileInfo, @NotNull ParseResult result) {
        System.out.printf("complete %s %s%s", fileInfo.getFilename(),
                result.isSucceeded() ? "successfully" : "unsuccessfully", System.getProperty("line.separator"));
    }

    @Override
    public boolean onRowError(@NotNull Throwable e) {
        System.err.println("error occurs");
        e.printStackTrace();
        throw new RuntimeException(e);
    }

    @Override
    public void onRowCountDetermined(long rowCount) {
        this.rowCount = Long.valueOf(rowCount).intValue();
    }
}
