/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import org.apache.parquet.io.PositionOutputStream;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFilePositionOutputStream extends PositionOutputStream {

    private final FileOutputStream fileOutputStream;
    private long position = 0L;

    public LocalFilePositionOutputStream(String fileName) throws FileNotFoundException {
        fileOutputStream = new FileOutputStream(fileName);
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        fileOutputStream.write(b);
        position++;
    }

    @Override
    public void close() throws IOException {
        fileOutputStream.close();
    }
}
