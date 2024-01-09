/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.derby

import com.hagoapp.f2t.ColumnTypeModifier
import java.sql.JDBCType
import java.sql.JDBCType.*

/**
 * Derby type string parser.
 *
 * @author suncj2
 * @since 0.8.5
 */
class Parsers {
    companion object {
        private const val TYPE_BOOLEAN_PREFIX = "BOOLEAN"
        private const val TYPE_INTEGER_PREFIX = "INTEGER"
        private const val TYPE_BIGINT_PREFIX = "BIGINT"
        private const val TYPE_SMALLINT_PREFIX = "SMALLINT"
        private const val TYPE_CHAR_PREFIX = "CHAR"
        private const val TYPE_VARCHAR_PREFIX = "VARCHAR"
        private const val TYPE_BIT_DATA_SUFFIX = "FOR BIT DATA"
        private const val TYPE_CLOB_PREFIX = "CLOB"
        private const val TYPE_BLOB_PREFIX = "BLOB"
        private const val TYPE_REAL_PREFIX = "REAL"
        private const val TYPE_DOUBLE_PREFIX = "DOUBLE"
        private const val TYPE_DECIMAL_PREFIX = "DECIMAL"
        private const val TYPE_TIMESTAMP_PREFIX = "TIMESTAMP"
        private const val TYPE_DATE_PREFIX = "DATE"
        private const val TYPE_TIME_PREFIX = "TIME"
        private const val TYPE_LONG_VARCHAR_PREFIX = "LONG VARCHAR"
        private const val TYPE_XML_PREFIX = "XML"
        private const val MODIFIER_NOT_NULL = "NOT NULL"

        private fun isNullable(colDataTypeString: String): Boolean {
            return !colDataTypeString.contains(MODIFIER_NOT_NULL)
        }

        @JvmField
        val integerParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_INTEGER_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    INTEGER, ColumnTypeModifier(
                        0, 0, 0, null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val booleanParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_BOOLEAN_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    BOOLEAN, ColumnTypeModifier(
                        0, 0, 0, null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val smallintParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_SMALLINT_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    SMALLINT, ColumnTypeModifier(
                        0, 0, 0, null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val bigintParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_BIGINT_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    BIGINT, ColumnTypeModifier(
                        0, 0, 0, null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val doubleParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_DOUBLE_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    DOUBLE, ColumnTypeModifier(
                        0, 0, 0, null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val realParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_REAL_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    FLOAT, ColumnTypeModifier(
                        0, 0, 0, null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val decimalParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_DECIMAL_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_DECIMAL_PREFIX\\((\\d+),(\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable decimal '$dataTypeString'")
                return Pair(
                    DECIMAL, ColumnTypeModifier(
                        0, r.groupValues[1].toInt(), r.groupValues[2].toInt(),
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val charParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_CHAR_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_CHAR_PREFIX\\s*\\((\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable char '$dataTypeString'")
                return Pair(
                    CHAR, ColumnTypeModifier(
                        r.groupValues[1].toInt(), 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val varcharParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_VARCHAR_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_VARCHAR_PREFIX\\s*\\((\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable varchar '$dataTypeString'")
                return Pair(
                    VARCHAR, ColumnTypeModifier(
                        r.groupValues[1].toInt(), 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val longVarcharParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_LONG_VARCHAR_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    LONGVARCHAR, ColumnTypeModifier(
                        32767, 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val charBitParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_CHAR_PREFIX
            }

            override fun supportedSuffix(): String {
                return TYPE_BIT_DATA_SUFFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_CHAR_PREFIX\\s*\\((\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable char bit '$dataTypeString'")
                return Pair(
                    CHAR, ColumnTypeModifier(
                        r.groupValues[1].toInt(), 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val varcharBitParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_VARCHAR_PREFIX
            }

            override fun supportedSuffix(): String {
                return TYPE_BIT_DATA_SUFFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_VARCHAR_PREFIX\\s*\\((\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable varchar bit '$dataTypeString'")
                return Pair(
                    VARCHAR, ColumnTypeModifier(
                        r.groupValues[1].toInt(), 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val longVarcharBitParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_LONG_VARCHAR_PREFIX
            }

            override fun supportedSuffix(): String {
                return TYPE_BIT_DATA_SUFFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    LONGVARCHAR, ColumnTypeModifier(
                        32767, 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val clobParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_CLOB_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_CLOB_PREFIX\\((\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable clob '$dataTypeString'")
                return Pair(
                    CLOB, ColumnTypeModifier(
                        r.groupValues[1].toInt(), 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val blobParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_BLOB_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                val r = Regex("$TYPE_BLOB_PREFIX\\((\\d+)\\)").find(dataTypeString)
                    ?: throw UnsupportedOperationException("Not recognizable blob '$dataTypeString'")
                return Pair(
                    BLOB, ColumnTypeModifier(
                        r.groupValues[1].toInt(), 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val xmlParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_XML_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    CLOB, ColumnTypeModifier(
                        Int.MAX_VALUE, 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val timestampParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_TIMESTAMP_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    TIMESTAMP, ColumnTypeModifier(
                        0, 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val timeParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_TIME_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    TIME, ColumnTypeModifier(
                        0, 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }

        @JvmField
        val dateParser = object : TypeParser.Parser {
            override fun supportedPrefix(): String {
                return TYPE_DATE_PREFIX
            }

            override fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier> {
                return Pair(
                    DATE, ColumnTypeModifier(
                        0, 0, 0,
                        null, false, isNullable(dataTypeString)
                    )
                )
            }
        }
    }
}
