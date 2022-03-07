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
            dbColumnDefinition: ColumnDefinition,
            vararg extra: String
        ): CompareColumnResult

        fun supportSourceTypes(): Set<JDBCType>
        fun supportDestinationTypes(): Set<JDBCType>

    }

    interface Transformer {
        fun transform(
            src: Any?,
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition,
            vararg extra: String
        ): Any?

        fun supportSourceTypes(): Set<JDBCType>
        fun supportDestinationTypes(): Set<JDBCType>
    }

    companion object {

        private val comparators = mutableMapOf<String, Comparator>()
        private val transformers = mutableMapOf<String, Transformer>()
        private val logger = F2TLogger.getLogger()

        init {
            registerComparatorsAndConverters(ColumnComparator::class.java.packageName)
        }

        /**
         * Allow user to register own registers and converters.
         *
         * @param packageNames  the package name to search for implementations.
         */
        fun registerComparatorsAndConverters(vararg packageNames: String) {
            for (packageName in packageNames) {
                Reflections(packageName, Scanners.SubTypes).getSubTypesOf(Comparator::class.java).forEach { clz ->
                    try {
                        logger.debug("${clz.canonicalName} is found for ColumnComparator")
                        val instance = clz.getConstructor().newInstance()
                        val sources = instance.supportSourceTypes().sorted()
                        val destinations = instance.supportDestinationTypes().sorted()
                        sources.forEach { src ->
                            destinations.forEach { dest ->
                                logger.debug("${clz.canonicalName} supports $src -> $dest")
                                val key = calcKey(src, dest)
                                if (comparators.containsKey(key)) {
                                    logger.warn(
                                        "comparator conflicted: $src -> $dest: ${clz.canonicalName} -- ${
                                            comparators.getValue(
                                                key
                                            )::class.java.canonicalName
                                        }"
                                    )
                                    logger.warn(
                                        "comparator: $src -> $dest: ${
                                            comparators.getValue(key)::class.java.canonicalName
                                        } is override"
                                    )
                                }
                                comparators[key] = instance
                            }
                        }
                    } catch (e: Exception) {
                        logger.error("error occurs in instantiating ${clz.canonicalName}")
                    }
                }

                Reflections(packageName, Scanners.SubTypes).getSubTypesOf(Transformer::class.java).forEach { clz ->
                    try {
                        logger.debug("${clz.canonicalName} is found for ColumnConverter")
                        val instance = clz.getConstructor().newInstance()
                        val sources = instance.supportSourceTypes().sorted()
                        val destinations = instance.supportDestinationTypes().sorted()
                        sources.forEach { src ->
                            destinations.forEach { dest ->
                                logger.debug("${clz.canonicalName} supports $src -> $dest")
                                val key = calcKey(src, dest)
                                if (transformers.containsKey(key)) {
                                    logger.warn(
                                        "converter conflicted: $src -> $dest: ${clz.canonicalName} -- ${
                                            transformers.getValue(
                                                key
                                            )::class.java.canonicalName
                                        }"
                                    )
                                    logger.warn(
                                        "converter: $src -> $dest: ${
                                            transformers.getValue(key)::class.java.canonicalName
                                        } is override"
                                    )
                                }
                                transformers[key] = instance
                            }
                        }
                    } catch (e: Exception) {
                        logger.error("error occurs in instantiating ${clz.canonicalName}")
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
                ?: throw UnsupportedOperationException("compare ${fileColumnDefinition.dataType} -> ${dbColumnDefinition.dataType} not supported")
            return comparator.dataCanLoadFrom(fileColumnDefinition, dbColumnDefinition)
        }

        fun transform(
            src: Any?,
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition,
            vararg extra: String
        ): Any? {
            val transformer = transformers[calcKey(fileColumnDefinition.dataType, dbColumnDefinition.dataType)]
            return if (transformer == null) {
                logger.warn("transform ${fileColumnDefinition.dataType} -> ${dbColumnDefinition.dataType} not supported")
                src
            } else {
                transformer.transform(src, fileColumnDefinition, dbColumnDefinition, *extra)
            }
        }
    }
}
