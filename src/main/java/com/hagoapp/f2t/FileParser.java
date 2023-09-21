/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.*;
import com.hagoapp.util.StackTraceWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * The generic event driven file parsing process. It accepts file information, create appropriate file reader and
 * try to determine its schema and read each data line until end. All registered observer can be notified for each
 * action. See <code>F2TProcess</code> as reference to see how to deal with notifications.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
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
    private final Logger logger = LoggerFactory.getLogger(FileParser.class);
    private FileTypeDeterminer determiner =
            new FileTypeDeterminer(FileColumnTypeDeterminer.Companion.getLeastTypeDeterminer());

    /**
     * Get current file column type determiner which will determine data type of each column.
     *
     * @return current file column type determiner
     */
    public FileTypeDeterminer getDeterminer() {
        return determiner;
    }

    /**
     * Set the <code>FileTypeDeterminer</code> instance to be used to help determine column types.
     *
     * @param determiner file column type determiner
     */
    public void setDeterminer(FileTypeDeterminer determiner) {
        this.determiner = determiner;
    }

    /**
     * Get the count of rows required to read to determine data type. A non-positive value indicated all rows.
     *
     * @return count of rows required to read to determine data type
     */
    public long getRowCountToInferType() {
        return rowCountToInferType;
    }

    /**
     * Set the count of rows required to read to determine data type. A non-positive value indicated all rows.
     *
     * @param rowCountToInferType count of rows required to read to determine data type
     */
    public void setRowCountToInferType(long rowCountToInferType) {
        this.rowCountToInferType = rowCountToInferType;
    }

    /**
     * Get the file information passed when constructing this instance.
     *
     * @return file information
     */
    public FileInfo getFileInfo() {
        return fileInfo;
    }

    /**
     * The constructor.
     *
     * @param fileInfo file information
     * @throws IOException if file not exists or error occurs while reading
     */
    public FileParser(FileInfo fileInfo) throws IOException {
        if (fileInfo == null) {
            throw new IOException("null file");
        }
        this.fileInfo = fileInfo;
        var f = new File(Objects.requireNonNull(fileInfo.getFilename()));
        if (!f.exists() || !f.canRead()) {
            throw new IOException(String.format("'%s' not existed or not accessible", fileInfo.getFilename()));
        }
    }

    /**
     * Register an observer.
     *
     * @param observer observer
     */
    public void addObserver(ParseObserver observer) {
        if (observer != null) {
            observers.add(observer);
        }
    }

    /**
     * Run the parse process with default options.
     */
    public void parse() {
        parse(new FileParserOption());
    }

    /**
     * Run the parse process with given options.
     *
     * @param option parse options
     */
    public void parse(FileParserOption option) {
        var result = new ParseResult();
        try (var reader = ReaderFactory.Companion.getReader(fileInfo)) {
            reader.setupTypeDeterminer(determiner);
            if (!option.isInferColumnTypes()) {
                reader.skipTypeInfer();
            }
            notifyObserver("onParseStart", fileInfo);
            reader.open(fileInfo);
            var rowNo = reader.getRowCount();
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
            var i = 0;
            while (reader.hasNext()) {
                if (!readOneRow(reader, result, i)) {
                    break;
                }
                i++;
            }
            if (rowNo == null) {
                notifyObserver("onRowCountDetermined", i);
            }
        } catch (Exception e) {
            notifyObserver("onError", e);
            result.addError(-1, e);
        } finally {
            endParse(result);
        }
    }

    private boolean readOneRow(Reader reader, ParseResult result, int i) {
        try {
            DataRow row = reader.next();
            notifyObserver("onRowRead", row);
            return true;
        } catch (Exception e) {
            result.addError(i, e);
            return observers.stream().anyMatch(observer -> observer.onRowError(e));
        }
    }

    private void endParse(ParseResult result) {
        result.end();
        notifyObserver("onParseComplete", fileInfo, result);
    }

    private void notifyObserver(String methodName, Object... params) {
        observers.forEach(observer -> {
            try {
                var method = methods.get(methodName);
                if (method != null) {
                    method.invoke(observer, params);
                } else {
                    logger.warn("callback {} of ParseObserver not found", methodName);
                }
            } catch (Exception e) {
                logger.error("callback {} of ParseObserver {} failed: {}", methodName,
                        observer.getClass().getCanonicalName(), e.getMessage());
                StackTraceWriter.writeToLogger(e, logger);
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
        var observer = new ExtractorObserver();
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
