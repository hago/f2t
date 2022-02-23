/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.F2TLogger
import com.hagoapp.f2t.FileColumnDefinition
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import java.sql.JDBCType

class ColumnComparator {

    interface Comparator {
        fun dataCanLoadFrom(
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition
        ): CompareColumnResult

        fun supportSourceTypes(): Set<JDBCType>
        fun supportDestinationTypes(): Set<JDBCType>

    }

    companion object {

        private val comparators = mutableMapOf<String, Comparator>()
        private val logger = F2TLogger.getLogger()

        init {
            Reflections(
                ColumnComparator::class.java.packageName,
                Scanners.SubTypes
            ).getSubTypesOf(Comparator::class.java).forEach { clz ->
                logger.debug("${clz.canonicalName} is found for ColumnComparator")
                val instance = clz.getConstructor().newInstance()
                val sources = instance.supportSourceTypes().sorted()
                val destinations = instance.supportDestinationTypes().sorted()
                sources.forEach { src ->
                    destinations.forEach { dest ->
                        logger.debug("${clz.canonicalName} supports $src -> $dest")
                        val key = calcKey(src, dest)
                        if (comparators.containsKey(key)) {
                            logger.warn("conflicted: $src -> $dest: ${clz.canonicalName} -- ${comparators[key]!!::class.java.canonicalName}")
                        }
                        comparators[key] = instance
                    }
                }
            }
        }

        private fun calcKey(src: JDBCType, dest: JDBCType): String {
            return "$src --> $dest"
        }

        fun compare(
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition
        ): CompareColumnResult {
            val comparator = comparators[calcKey(fileColumnDefinition.dataType, dbColumnDefinition.dataType)]
                ?: throw UnsupportedOperationException("${fileColumnDefinition.dataType} -> ${dbColumnDefinition.dataType} not supported")
            return comparator.dataCanLoadFrom(fileColumnDefinition, dbColumnDefinition)
        }
    }
}
