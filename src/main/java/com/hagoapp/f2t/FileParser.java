/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.*;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        File f = new File(fileInfo.getFilename());
        if (!f.exists() || !f.canRead()) {
            throw new IOException(String.format("'%s' not existed or not accessible", fileInfo.getFilename()));
        }
    }

    public void addWatcher(ParseObserver watcher) {
        if (watcher != null) {
            observers.add(watcher);
        }
    }

    public void run() {
        run(new FileParserOption());
    }

    public void run(FileParserOption option) {
        try (Reader reader = ReaderFactory.getReader(fileInfo)) {
            notifyObserver("onParseStart", fileInfo);
            ParseResult result = new ParseResult();
            reader.open(fileInfo);
            Integer rowNo = reader.getRowCount();
            if (rowNo != null) {
                notifyObserver("onRowCountDetermined", rowNo);
            }
            List<ColumnDefinition> definitions = reader.findColumns();
            notifyObserver("onColumnsParsed", definitions);
            if (!option.isInferColumnTypes()) {
                return;
            }
            List<ColumnDefinition> typedDefinitions = reader.inferColumnTypes(rowCountToInferType);
            notifyObserver("onColumnTypeDetermined", definitions);
            if (!option.isReadData()) {
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
            notifyObserver("onParseComplete", fileInfo, result);
        } catch (IOException e) {
            //
        }
    }

    private void notifyObserver(String methodName, Object... params) {
        observers.forEach(observer -> {
            try {
                Method method = methods.get(methodName);
                if (method != null) {
                    method.invoke(observer, params);
                    logger.error("callback {} of ParseObserver {} invoked", methodName,
                            observer.getClass().getCanonicalName());
                } else {
                    logger.error("callback {} of ParseObserver not found", methodName);
                }
            } catch (Throwable e) {
                logger.error("callback {} of ParseObserver {} failed: {}", methodName,
                        observer.getClass().getCanonicalName(), e.getMessage());
                e.printStackTrace();
            }
        });
    }

    public void getDataContainer() {

    }
}
