/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.util

import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import java.sql.JDBCType
import java.sql.JDBCType.*
import java.sql.Time
import java.time.*
import java.util.*

/**
 * Utility class for parquet file, using apache impl.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
class ParquetTypeUtils {
    companion object {

        /**
         * This interface is for user to define their own converter from Avro type to JDBC Type.
         */
        interface TypeToJDBCTypeMapper {
            fun mapToJDBCTypes(): Set<JDBCType>
            fun supportedTypes(): Set<Type>
        }

        private val extraMappers = mutableMapOf<Type, TypeToJDBCTypeMapper>()

        /**
         * Register customized avro type to JDBC type mapper.
         *
         * @param mapper customized mapper
         */
        fun registerExtraMapper(mapper: TypeToJDBCTypeMapper) {
            for (x in mapper.supportedTypes()) {
                extraMappers[x] = mapper
            }
        }

        /**
         * Map JDBCType to corresponding avro type.
         *
         * @param input JDBC type
         * @return avro type name
         */
        @JvmStatic
        fun mapToAvroType(input: JDBCType): String {
            return when (input) {
                TINYINT, SMALLINT, INTEGER -> "int"
                BIGINT -> "long"
                BOOLEAN -> "boolean"
                FLOAT -> "float"
                DOUBLE, DECIMAL -> "double"
                BINARY, VARBINARY -> "bytes"
                TIME, TIME_WITH_TIMEZONE, DATE, TIMESTAMP_WITH_TIMEZONE, TIMESTAMP,
                CHAR, VARCHAR, CLOB, NCHAR, NVARCHAR, NCLOB -> "string"

                else -> throw UnsupportedOperationException("unsupported type $input")
            }
        }

        /**
         * Map avro type to JDBC type.
         *
         * @param input avro type
         * @return JDBC type
         */
        @JvmStatic
        fun mapAvroTypeToJDBCType(input: Type): JDBCType {
            return when (input) {
                Type.INT -> INTEGER
                Type.LONG -> BIGINT
                Type.STRING -> CLOB
                Type.FLOAT -> FLOAT
                Type.DOUBLE -> DOUBLE
                Type.BYTES -> VARBINARY
                Type.BOOLEAN -> BOOLEAN
                else -> throw UnsupportedOperationException("unsupported type $input")
            }
        }

        /**
         * Guess all possible JDBC types those can be used to present value in given avro field. It will consider
         * explicit avro type defined in avro field, then consider the actual value if the avro type is generic
         * string.
         *
         * @param field avro field
         * @return set of possible JDBC types
         */
        @JvmStatic
        fun guessJDBCType(field: Field): Set<JDBCType> {
            val type = field.schema().type
            val ret = mutableSetOf<JDBCType>()
            if (extraMappers.containsKey(type)) {
                ret.addAll(extraMappers.getValue(type).mapToJDBCTypes())
            }
            when (type) {
                Type.BOOLEAN -> ret.addAll(setOf(BOOLEAN, TINYINT))
                Type.DOUBLE -> ret.add(DOUBLE)
                Type.FLOAT -> ret.add(FLOAT)
                Type.BYTES -> ret.add(VARBINARY)
                Type.INT -> ret.addAll(setOf(INTEGER, BIGINT))
                Type.LONG -> ret.add(BIGINT)
                Type.STRING -> JDBCTypeUtils.guessTypes(field.name())
                else -> JDBCTypeUtils.guessTypes(field.name())
            }
            return ret
        }

        @JvmStatic
        fun mapJdbcTypeToPrimitiveTypeName(jdbcType: JDBCType): PrimitiveTypeName {
            return when (jdbcType) {
                INTEGER, DATE, TIME, TIME_WITH_TIMEZONE -> PrimitiveTypeName.INT32
                BIGINT, TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> PrimitiveTypeName.INT64
                FLOAT -> PrimitiveTypeName.FLOAT
                DOUBLE -> PrimitiveTypeName.DOUBLE
                BOOLEAN -> PrimitiveTypeName.BOOLEAN
                else -> PrimitiveTypeName.BINARY
            }
        }

        @JvmStatic
        fun mapJdbcTypeToLogicalAnnotation(jdbcType: JDBCType): LogicalTypeAnnotation? {
            return when (jdbcType) {
                INTEGER -> LogicalTypeAnnotation.intType(32, true)
                BIGINT -> LogicalTypeAnnotation.intType(64, true)
                FLOAT -> null
                DOUBLE -> null
                BOOLEAN -> null
                DATE -> LogicalTypeAnnotation.dateType()
                TIME -> LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)
                TIME_WITH_TIMEZONE -> LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MILLIS)
                TIMESTAMP -> LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)
                TIMESTAMP_WITH_TIMEZONE -> LogicalTypeAnnotation.timestampType(
                    false,
                    LogicalTypeAnnotation.TimeUnit.MILLIS
                )
                BINARY, VARBINARY, LONGVARBINARY -> null
                else -> LogicalTypeAnnotation.stringType()
            }
        }

        @JvmStatic
        @Throws(UnsupportedOperationException::class)
        fun convertJavaDataForParquetInt(data: Any): Int {
            return when(data) {
                is Int -> data
                is LocalDate -> convertLocalDateForParquet(data)
                is LocalTime -> convertLocalTimeForParquet(data)
                is Time -> convertTimeForParquet(data)
                else -> throw UnsupportedOperationException("Not in type: int, date or time")
            }
        }

        @JvmStatic
        @Throws(UnsupportedOperationException::class)
        fun convertJavaDataForParquetLong(data: Any): Long {
            return when(data) {
                is Long -> data
                is Date -> convertDateForParquet(data)
                is LocalDateTime -> convertLocalDateTimeForParquet(data)
                is ZonedDateTime -> convertZonedDateTimeForParquet(data)
                else -> throw UnsupportedOperationException("Not in type: long, date, localdatetime or zoneddatetime")
            }
        }

        @JvmStatic
        @Throws(UnsupportedOperationException::class)
        @Suppress("UNCHECKED_CAST")
        fun convertJavaDataForParquetBinary(data: Any): Binary {
            return when(data) {
                is ByteArray -> Binary.fromConstantByteArray(data)
                is Array<*> -> {
                    val b = data as Array<Byte>
                    Binary.fromConstantByteArray(b.toByteArray())
                }
                is String -> Binary.fromString(data)
                else -> throw UnsupportedOperationException("Not in type: bytearray, byte[]")
            }
        }

        @JvmStatic
        fun convertZonedDateTimeForParquet(src: ZonedDateTime): Long {
            return src.toInstant().toEpochMilli()
        }

        @JvmStatic
        fun convertLocalDateTimeForParquet(src: LocalDateTime, zoneId: ZoneId = ZoneId.systemDefault()): Long {
            return convertZonedDateTimeForParquet(src.atZone(zoneId))
        }

        @JvmStatic
        fun convertDateForParquet(src: Date): Long {
            return src.toInstant().toEpochMilli()
        }

        @JvmStatic
        fun convertLocalDateForParquet(src: LocalDate): Int {
            return src.toEpochDay().toInt()
        }

        @JvmStatic
        fun convertLocalTimeForParquet(src: LocalTime): Int {
            return src.toSecondOfDay()
        }

        @JvmStatic
        fun convertTimeForParquet(src: Time): Int {
            return convertLocalTimeForParquet(src.toLocalTime())
        }
    }
}
