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

import com.hagoapp.f2t.datafile.FileInfo;
import com.hagoapp.f2t.datafile.ParseResult;
import kotlin.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileTestObserver implements ParseObserver {
    private int rowCount;
    private final Map<String, Pair<FileColumnDefinition, Integer>> columns = new HashMap<>();
    private boolean rowDetail = false;
    private final Logger logger = LoggerFactory.getLogger(FileTestObserver.class);

    public int getRowCount() {
        logger.debug("getRowCount {}", rowCount);
        return rowCount;
    }

    public Map<String, Pair<FileColumnDefinition, Integer>> getColumns() {
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
    public void onColumnsParsed(@NotNull List<FileColumnDefinition> columnDefinitionList) {
        logger.info("column parsed");
        for (int i = 0; i < columnDefinitionList.size(); i++) {
            var def = columnDefinitionList.get(i);
            columns.put(def.getName(), new Pair<>(def, i));
        }
    }

    @Override
    public void onColumnTypeDetermined(@NotNull List<FileColumnDefinition> columnDefinitionList) {
        logger.info("column definition determined");
        logger.info("columns: {}", columnDefinitionList);
        for (int i = 0; i < columnDefinitionList.size(); i++) {
            var def = columnDefinitionList.get(i);
            columns.put(def.getName(), new Pair<>(def, i));
        }
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
