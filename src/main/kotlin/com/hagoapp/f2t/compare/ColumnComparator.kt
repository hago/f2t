/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.compare

import com.hagoapp.f2t.ColumnDefinition
import com.hagoapp.f2t.FileColumnDefinition
import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.slf4j.LoggerFactory
import java.sql.JDBCType

/**
 * Convenient utility to compare between data file column definition and database table column definition.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class ColumnComparator {

    companion object {

        private val comparators = mutableMapOf<String, TypedColumnComparator>()
        private val transformers = mutableMapOf<String, TypedColumnTransformer>()
        private val logger = LoggerFactory.getLogger(ColumnComparator::class.java)

        init {
            registerComparatorsAndConverters(ColumnComparator::class.java.packageName)
        }

        /**
         * Allow user to register own registers and converters.
         *
         * @param packageNames  the package name to search for implementations.
         */
        fun registerComparatorsAndConverters(vararg packageNames: String) {
            registerComparators(*packageNames)
            registerTransformers(*packageNames)
        }

        private fun registerComparators(vararg packageNames: String) {
            for (packageName in packageNames) {
                val typedColumnComparators = Reflections(packageName, Scanners.SubTypes)
                    .getSubTypesOf(TypedColumnComparator::class.java)
                for (clz in typedColumnComparators) {
                    val instance: TypedColumnComparator
                    try {
                        logger.debug("${clz.canonicalName} is found for ColumnComparator")
                        instance = clz.getConstructor().newInstance()
                    } catch (e: Exception) {
                        logger.error("error occurs in instantiating ${clz.canonicalName}")
                        continue
                    }
                    val sources = instance.supportSourceTypes().sorted()
                    val destinations = instance.supportDestinationTypes().sorted()
                    sources.forEach { src ->
                        destinations.forEach { dest ->
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
                }
            }
        }

        private fun registerTransformers(vararg packageNames: String) {
            for (packageName in packageNames) {
                val typedColumnTransformers = Reflections(packageName, Scanners.SubTypes)
                    .getSubTypesOf(TypedColumnTransformer::class.java)
                for (clz in typedColumnTransformers) {
                    val instance: TypedColumnTransformer
                    try {
                        logger.debug("${clz.canonicalName} is found for ColumnConverter")
                        instance = clz.getConstructor().newInstance()
                    } catch (e: Exception) {
                        logger.error("error occurs in instantiating ${clz.canonicalName}")
                        continue
                    }
                    val sources = instance.supportSourceTypes().sorted()
                    val destinations = instance.supportDestinationTypes().sorted()
                    sources.forEach { src ->
                        destinations.forEach { dest ->
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
                }
            }
        }

        private fun calcKey(src: JDBCType, dest: JDBCType): String {
            return "$src --> $dest"
        }

        /**
         * Compare between data file column definition and database table column definition.
         *
         * @param fileColumnDefinition  file column definition
         * @param dbColumnDefinition    database column definition
         * @return compare result
         */
        fun compare(
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition
        ): CompareColumnResult {
            val comparator = comparators[calcKey(fileColumnDefinition.dataType, dbColumnDefinition.dataType)]
                ?: throw UnsupportedOperationException("compare ${fileColumnDefinition.dataType} -> ${dbColumnDefinition.dataType} not supported")
            return comparator.dataCanLoadFrom(fileColumnDefinition, dbColumnDefinition)
        }

        private val failsafeTransformer = object : TypedColumnTransformer {
            override fun transform(
                src: Any?,
                fileColumnDefinition: FileColumnDefinition,
                dbColumnDefinition: ColumnDefinition,
                vararg extra: String
            ): Any? {
                return src
            }

            override fun supportSourceTypes(): Set<JDBCType> {
                return setOf()
            }

            override fun supportDestinationTypes(): Set<JDBCType> {
                return setOf()
            }
        }

        /**
         * Create transformer to deal with data from file column to one fit for target database table column.
         *
         * @param fileColumnDefinition  file column definition
         * @param dbColumnDefinition    database column definition
         * @return  transformer instance
         */
        fun getTransformer(
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition
        ): TypedColumnTransformer {
            val ret = transformers[calcKey(fileColumnDefinition.dataType, dbColumnDefinition.dataType)]
            return if (ret == null) {
                logger.error("no transform ${fileColumnDefinition.dataType} -> ${dbColumnDefinition.dataType}, default used")
                failsafeTransformer
            } else {
                ret
            }
        }

        /**
         * Transform data from file column to one fit for target database table column.
         *
         * @param src   source data
         * @param fileColumnDefinition  file column definition
         * @param dbColumnDefinition    database column definition
         * @param extra additional parameters
         * @return transformed value
         */
        fun transform(
            src: Any?,
            fileColumnDefinition: FileColumnDefinition,
            dbColumnDefinition: ColumnDefinition,
            vararg extra: String
        ): Any? {
            val transformer = getTransformer(fileColumnDefinition, dbColumnDefinition)
            return transformer.transform(src, fileColumnDefinition, dbColumnDefinition, *extra)
        }
    }
}
