/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import com.hagoapp.f2t.datafile.*;

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

    public long getRowCountToInferType() {
        return rowCountToInferType;
    }

    public void setRowCountToInferType(long rowCountToInferType) {
        this.rowCountToInferType = rowCountToInferType;
    }

    public FileParser(FileInfo fileInfo) throws IOException {
        if ((fileInfo == null) || (fileInfo.getFilename() == null)) {
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
        try (Reader reader = ReaderFactory.getReader(fileInfo)) {
            notifyObserver("onParseStart");
            ParseResult result = new ParseResult();
            reader.open(fileInfo);
            List<ColumnDefinition> definitions = reader.findColumns();
            notifyObserver("onColumnsParsed", definitions);
            List<ColumnDefinition> typedDefinitions = reader.inferColumnTypes(rowCountToInferType);
            notifyObserver("onColumnTypeDetermined", definitions);
            long rowNo = 0;
            while (reader.hasNext()) {
                try {
                    DataRow row = reader.next();
                    notifyObserver("onRowRead", row);
                    rowNo++;
                } catch (Throwable e) {
                    result.addError(rowNo, e);
                    notifyObserver("onRowError", e);
                }
            }
            notifyObserver("onParseComplete", result);
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
                }
            } catch (Throwable ignored) {
                //
            }
        });
    }
}
