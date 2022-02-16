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

import com.hagoapp.f2t.database.definition.ColumnDefinition;
import com.hagoapp.f2t.datafile.FileInfo;
import com.hagoapp.f2t.datafile.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.sql.JDBCType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileTestObserver implements ParseObserver {
    private int rowCount;
    private Map<String, JDBCType> columns;
    private boolean rowDetail = false;
    private final Logger logger = F2TLogger.getLogger();

    public int getRowCount() {
        logger.debug("getRowCount {}", rowCount);
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
        logger.info("start parsing {}", fileInfo.getFilename());
    }

    @Override
    public void onColumnsParsed(@NotNull List<ColumnDefinition> columnDefinitionList) {
        logger.info("column parsed");
        columns = columnDefinitionList.stream().collect(Collectors.toMap(
                ColumnDefinition::getName,
                col -> JDBCType.NULL
        ));
    }

    @Override
    public void onColumnTypeDetermined(@NotNull List<ColumnDefinition> columnDefinitionList) {
        logger.info("column definition determined");
        columns = columnDefinitionList.stream().collect(Collectors.toMap(
                ColumnDefinition::getName,
                col -> col.getInferredType() != null ? col.getInferredType() : JDBCType.NULL
        ));
    }

    @Override
    public void onRowRead(@NotNull DataRow row) {
        logger.info("row {} get", row.getRowNo());
        if (rowDetail) {
            logger.info(row.toString());
        }
    }

    @Override
    public void onParseComplete(@NotNull FileInfo fileInfo, @NotNull ParseResult result) {
        logger.info("complete parsing {} {}", fileInfo.getFilename(),
                result.isSucceeded() ? "successfully" : "unsuccessfully");
    }

    @Override
    public boolean onRowError(@NotNull Throwable e) {
        logger.error("error occurs: {}", e.getMessage());
        e.printStackTrace();
        throw new RuntimeException(e);
    }

    @Override
    public void onRowCountDetermined(int rowCount) {
        logger.info("row count determined: {}", rowCount);
        this.rowCount = rowCount;
    }
}
