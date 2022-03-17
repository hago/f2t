/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

public class FileParser {

    private static final Map<String, Method> methods = new HashMap<>();

    static {
        for (Method method : ParseObserver.class.getDeclaredMethods()) {
            methods.put(method.getName(), method);
        }
    }

    private final FileInfo fileInfo;
    private final List<ParseObserver> observers = new ArrayList<>();
    private long rowCountToInferType = -1;
    private final Logger logger = F2TLogger.getLogger();
    private final Map<String, DataTypeDeterminer> columnDeterminerMap = new HashMap<>();
    private DataTypeDeterminer defaultDeterminer = new LeastTypeDeterminer();

    public DataTypeDeterminer getDefaultDeterminer() {
        return defaultDeterminer;
    }

    public void setDefaultDeterminer(DataTypeDeterminer defaultDeterminer) {
        this.defaultDeterminer = defaultDeterminer;
    }

    public void setupColumnDeterminer(String column, DataTypeDeterminer determiner) {
        columnDeterminerMap.put(column, determiner);
    }

    public long getRowCountToInferType() {
        return rowCountToInferType;
    }

    public void setRowCountToInferType(long rowCountToInferType) {
        this.rowCountToInferType = rowCountToInferType;
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public FileParser(FileInfo fileInfo) throws IOException {
        if (fileInfo == null) {
            throw new IOException("null file");
        }
        this.fileInfo = fileInfo;
        File f = new File(Objects.requireNonNull(fileInfo.getFilename()));
        if (!f.exists() || !f.canRead()) {
            throw new IOException(String.format("'%s' not existed or not accessible", fileInfo.getFilename()));
        }
    }

    public void addObserver(ParseObserver observer) {
        if (observer != null) {
            observers.add(observer);
        }
    }

    public void parse() {
        parse(new FileParserOption());
    }

    public void parse(FileParserOption option) {
        ParseResult result = new ParseResult();
        try (Reader reader = ReaderFactory.Companion.getReader(fileInfo)) {
            reader.setupTypeDeterminer(defaultDeterminer);
            columnDeterminerMap.forEach(reader::setupColumnTypeDeterminer);
            notifyObserver("onParseStart", fileInfo);
            reader.open(fileInfo);
            Integer rowNo = reader.getRowCount();
            if (rowNo != null) {
                notifyObserver("onRowCountDetermined", rowNo);
            }
            List<FileColumnDefinition> definitions = reader.findColumns();
            notifyObserver("onColumnsParsed", definitions);
            if (!option.isInferColumnTypes()) {
                endParse(result);
                return;
            }
            List<FileColumnDefinition> typedDefinitions = reader.inferColumnTypes(rowCountToInferType);
            notifyObserver("onColumnTypeDetermined", typedDefinitions);
            if (!option.isReadData()) {
                endParse(result);
                return;
            }
            int i = 0;
            while (reader.hasNext()) {
                try {
                    DataRow row = reader.next();
                    notifyObserver("onRowRead", row);
                    i++;
                } catch (Throwable e) {
                    result.addError(i, e);
                    if (observers.stream().noneMatch(observer -> observer.onRowError(e))) {
                        break;
                    }
                }
            }
            if (rowNo == null) {
                notifyObserver("onRowCountDetermined", i);
            }
        } catch (Throwable e) {
            notifyObserver("onError", e);
            result.addError(-1, e);
        } finally {
            endParse(result);
        }
    }

    private void endParse(ParseResult result) {
        result.end();
        notifyObserver("onParseComplete", fileInfo, result);
    }

    private void notifyObserver(String methodName, Object... params) {
        observers.forEach(observer -> {
            try {
                Method method = methods.get(methodName);
                if (method != null) {
                    method.invoke(observer, params);
                    //logger.debug("callback {} of ParseObserver {} invoked", methodName,
                    //        observer.getClass().getCanonicalName());
                } else {
                    logger.warn("callback {} of ParseObserver not found", methodName);
                }
            } catch (Throwable e) {
                logger.error("callback {} of ParseObserver {} failed: {}", methodName,
                        observer.getClass().getCanonicalName(), e.getMessage());
                e.printStackTrace();
            }
        });
    }

    /**
     * Fetch data and concluded column definitions into a <Code>DataTable</Code> object.
     *
     * @return <Code>DataTable</Code> object including column definition and data
     * @throws F2TException if anything wrong
     */
    public DataTable<FileColumnDefinition> extractData() throws F2TException {
        ExtractorObserver observer = new ExtractorObserver();
        this.addObserver(observer);
        parse();
        return observer.getData();
    }

    private static class ExtractorObserver implements ParseObserver {
        private List<FileColumnDefinition> columns = null;
        private final List<DataRow> rows = new ArrayList<>();
        private Throwable error;

        @Override
        public void onColumnTypeDetermined(@NotNull List<FileColumnDefinition> columnDefinitionList) {
            columns = columnDefinitionList;
            columns.sort(Comparator.comparingInt(FileColumnDefinition::getOrder));
        }

        @Override
        public void onRowRead(@NotNull DataRow row) {
            rows.add(row);
        }

        @Override
        public boolean onRowError(@NotNull Throwable e) {
            error = e;
            return false;
        }

        public DataTable<FileColumnDefinition> getData() throws F2TException {
            if (error != null) {
                throw new F2TException("Error occurs during extracting: " + error.getMessage(), error);
            }
            if (columns == null) {
                throw new F2TException("No data definition set");
            }
            return new DataTable<>(columns, rows);
        }
    }
}
