/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile

import com.google.gson.GsonBuilder
import com.hagoapp.f2t.F2TException
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream

/**
 * A factory class to create reader based on <code>FileInfo</code> passed in.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
class FileInfoReader {
    companion object {

        private val fileInfoMap = mutableMapOf<Int, Class<out FileInfo>>()
        private val fileInfoExtMap = mutableMapOf<String, Class<out FileInfo>>()
        private val logger = LoggerFactory.getLogger(FileInfoReader::class.java)

        init {
            val r = Reflections(F2TException::class.java.packageName, Scanners.SubTypes)
            r.getSubTypesOf(FileInfo::class.java).forEach { clz ->
                val constructor = clz.getConstructor()
                val template = constructor.newInstance()
                fileInfoMap[template.type] = clz
                template.getSupportedFileExtNames().forEach { ext ->
                    fileInfoExtMap[ext] = clz
                }
                logger.info("FileInfo reader registered for type ${template.type}")
            }
        }

        /**
         * Create FileInfo object from stream.
         *
         * @param stream stream containing file information
         * @return data file information
         */
        fun createFileInfo(stream: InputStream): FileInfo {
            return createFileInfo(stream.readAllBytes())
        }

        /**
         * Create FileInfo object from byte array.
         *
         * @param content bytes containing file information
         * @return data file information
         */
        fun createFileInfo(content: ByteArray): FileInfo {
            return json2FileInfo(String(content))
        }

        /**
         * Create FileInfo object from file.
         *
         * @param filename json file containing file information
         * @return data file information
         */
        fun createFileInfo(filename: String): FileInfo {
            try {
                FileInputStream(filename).use {
                    return createFileInfo(it)
                }
            } catch (e: IOException) {
                throw F2TException("Load FileInfo object from file $filename failed", e)
            }
        }

        /**
         * Create FileInfo object from json string.
         *
         * @param content json containing file information
         * @return data file information
         */
        fun json2FileInfo(content: String): FileInfo {
            val gson = GsonBuilder().create()
            val base = gson.fromJson(content, FileInfo::class.java)
            val clz = when {
                fileInfoMap.containsKey(base.type) -> fileInfoMap[base.type]
                else -> getConcreteFileInfoByName(base.filename)
            } ?: throw F2TException("FileInfo type ${base.type} unknown")
            return gson.fromJson(content, clz)
        }

        /**
         * Create FileInfo object from file information stored in map.
         *
         * @param content json containing file information
         * @return data file information
         */
        fun json2FileInfo(content: Map<String, Any?>): FileInfo {
            return json2FileInfo(GsonBuilder().create().toJson(content))
        }

        private fun getConcreteFileInfoByName(fn: String?): Class<out FileInfo>? {
            fn ?: return null
            val ext = getFileExtension(fn).lowercase().trim()
            return fileInfoExtMap[ext]
        }

        private fun getFileExtension(fn: String): String {
            val parts = fn.split('.')
            return if (parts.size == 1) "" else parts.last()
        }
    }
}
