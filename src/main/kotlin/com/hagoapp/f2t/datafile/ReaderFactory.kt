/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.hagoapp.f2t.datafile

import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.F2TLogger
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import java.lang.reflect.Constructor

object ReaderFactory {

    private val readerMap = mutableMapOf<Int, Constructor<out Reader>>()
    private val logger = F2TLogger.getLogger()

    init {
        val r = Reflections(F2TException::class.java.packageName, Scanners.SubTypes)
        r.getSubTypesOf(Reader::class.java).forEach { clz ->
            val constructor = clz.getConstructor()
            val template = constructor.newInstance()
            template.getSupportedFileType().forEach { fileType ->
                readerMap[fileType] = constructor
                logger.info("Data file reader registered for type $fileType")
            }
        }
    }

    @JvmStatic
    fun getReader(fileInfo: FileInfo): Reader {
        return getReader(fileInfo, false)
    }

    @JvmStatic
    fun getReader(fileInfo: FileInfo, skipTypeInfer: Boolean = false): Reader {
        val constructor = readerMap[fileInfo.type] ?: throw F2TException("file type ${fileInfo.type} is not supported")
        val reader = constructor.newInstance()
        if (skipTypeInfer) {
            reader.skipTypeInfer()
        }
        return reader
    }
}
