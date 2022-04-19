/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import org.apache.parquet.io.PositionOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * A simple implementation of Hadoop's <code>PositionOutputStream</code>.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
public class LocalFilePositionOutputStream extends PositionOutputStream {

    private final FileOutputStream fileOutputStream;
    private long position = 0L;

    public LocalFilePositionOutputStream(String fileName) throws FileNotFoundException {
        var f = new File(fileName);
        fileOutputStream = new FileOutputStream(f);
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
