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

class ReaderFactory {

    companion object {
        private val readerMap = mutableMapOf<Int, Constructor<out Reader>>()
        private val logger = F2TLogger.getLogger()

        init {
            registerPackageName(F2TException::class.java.packageName)
        }

        /**
         * Register package names to find data file readers.
         *
         * @param packageNames  package names to search in
         */
        fun registerPackageName(vararg packageNames: String) {
            for (packageName in packageNames) {
                val r = Reflections(packageName, Scanners.SubTypes)
                r.getSubTypesOf(Reader::class.java).forEach { clz ->
                    val constructor = clz.getConstructor()
                    val template = constructor.newInstance()
                    template.getSupportedFileType().forEach { fileType ->
                        readerMap[fileType] = constructor
                        logger.info("Data file reader registered for type $fileType")
                    }
                }
            }
        }

        /**
         * Build a file reader according file information config and infer column types automatically.
         *
         * @param fileInfo Descendants of <Code>FileInfo</code> config
         * @return Reader instance
         */
        fun getReader(fileInfo: FileInfo): Reader {
            return getReader(fileInfo, false)
        }

        /**
         * Build a file reader according file information config and may skip column types inferring according
         * invoker's intention.
         *
         * @param fileInfo Descendants of <Code>FileInfo</code> config
         * @param skipTypeInfer whether to skip type inferring
         * @return Reader instance
         */
        fun getReader(fileInfo: FileInfo, skipTypeInfer: Boolean = false): Reader {
            val constructor =
                readerMap[fileInfo.type] ?: throw F2TException("file type ${fileInfo.type} is not supported")
            val reader = constructor.newInstance()
            if (skipTypeInfer) {
                reader.skipTypeInfer()
            }
            return reader
        }
    }
}
