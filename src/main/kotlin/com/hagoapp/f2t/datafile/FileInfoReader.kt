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
import org.reflections.scanners.Scanners
import java.io.FileInputStream
import java.io.InputStream

class FileInfoReader {
    companion object {

        private val fileInfoMap = mutableMapOf<Int, Class<out FileInfo>>()
        private val fileInfoExtMap = mutableMapOf<String, Class<out FileInfo>>()
        private val logger = F2TLogger.getLogger()

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

        fun createFileInfo(stream: InputStream): FileInfo {
            return createFileInfo(stream.readAllBytes())
        }

        fun createFileInfo(content: ByteArray): FileInfo {
            return json2FileInfo(String(content))
        }

        fun createFileInfo(filename: String): FileInfo {
            try {
                FileInputStream(filename).use {
                    return createFileInfo(it)
                }
            } catch (e: Throwable) {
                throw F2TException("Load FileInfo object from file $filename failed", e)
            }
        }

        fun json2FileInfo(content: String): FileInfo {
            val gson = GsonBuilder().create()
            val base = gson.fromJson(content, FileInfo::class.java)
            val clz = when {
                fileInfoMap.containsKey(base.type) -> fileInfoMap[base.type]
                else -> getConcreteFileInfoByName(base.filename)
            } ?: throw F2TException("FileInfo type ${base.type} unknown")
            return gson.fromJson(content, clz)
        }

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
