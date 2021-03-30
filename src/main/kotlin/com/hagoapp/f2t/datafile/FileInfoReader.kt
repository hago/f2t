/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.google.gson.GsonBuilder
import com.hagoapp.f2t.F2TException
import com.hagoapp.f2t.F2TLogger
import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import java.io.FileInputStream
import java.io.InputStream

class FileInfoReader {
    companion object {

        private val fileInfoMap = mutableMapOf<Int, Class<out FileInfo>>()
        private val logger = F2TLogger.getLogger()

        init {
            logger.debug("1")
            val r = Reflections(F2TException::class.java.packageName, SubTypesScanner())
            logger.debug("2")
            r.getSubTypesOf(FileInfo::class.java).forEach { clz ->
                val constructor = clz.getConstructor(String::class.java)
                logger.debug("3")
                val template = constructor.newInstance("")
                logger.debug("4")
                fileInfoMap[template.type] = clz
                logger.info("FileInfo reader registered for type ${template.type}")
            }
        }

        @JvmStatic
        fun createFileInfo(stream: InputStream): FileInfo {
            return createFileInfo(stream.readAllBytes())
        }

        @JvmStatic
        fun createFileInfo(content: ByteArray): FileInfo {
            return json2FileInfo(String(content))
        }

        @JvmStatic
        fun createFileInfo(filename: String): FileInfo {
            try {
                FileInputStream(filename).use {
                    return createFileInfo(it)
                }
            } catch (e: Throwable) {
                throw F2TException("Load FileInfo object from file $filename failed", e)
            }
        }

        @JvmStatic
        fun json2FileInfo(content: String): FileInfo {
            val gson = GsonBuilder().create()
            val base = gson.fromJson(content, FileInfo::class.java)
            val clz = fileInfoMap[base.type] ?: throw F2TException("FileInfo type ${base.type} unknown")
            return gson.fromJson(content, clz)
        }
    }
}
