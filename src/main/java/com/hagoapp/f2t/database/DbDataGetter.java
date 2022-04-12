/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * The interface to define a method to read a parameterized value from JDBC result set using given column name.
 *
 * @param <T> type of returned value
 * @author Chaojun Sun
 * @since 0.6
 */
public interface DbDataGetter<T> {
    /**
     * The method to read a parameterized value from JDBC result set using given column name.
     *
     * @param resultSet A JDBC result set
     * @param column    column name
     * @return typed value from database
     * @throws SQLException if anything wrong with database
     * @throws IOException  if anything wrong with IO
     */
    T getTypedValue(ResultSet resultSet, String column) throws SQLException, IOException;

    /**
     * Predefined string reader implementation.
     */
    DbDataGetter<String> StringDataGetter = ResultSet::getString;
    /**
     * Predefined MBCS string reader implementation.
     */
    DbDataGetter<String> NStringDataGetter = ResultSet::getNString;
    /**
     * Predefined integer reader implementation.
     */
    DbDataGetter<Integer> IntDataGetter = ResultSet::getInt;
    /**
     * Predefined byte/small integer reader implementation.
     */
    DbDataGetter<Byte> TinyIntDataGetter = ResultSet::getByte;
    /**
     * Predefined short integer reader implementation.
     */
    DbDataGetter<Short> ShortDataGetter = ResultSet::getShort;
    /**
     * Predefined long integer reader implementation.
     */
    DbDataGetter<Long> LongDataGetter = ResultSet::getLong;
    /**
     * Predefined float reader implementation.
     */
    DbDataGetter<Float> FloatDataGetter = ResultSet::getFloat;
    /**
     * Predefined double precision reader implementation.
     */
    DbDataGetter<Double> DoubleDataGetter = ResultSet::getDouble;
    /**
     * Predefined boolean reader implementation.
     */
    DbDataGetter<Boolean> BooleanDataGetter = ResultSet::getBoolean;
    /**
     * Predefined timestamp reader implementation.
     */
    DbDataGetter<Timestamp> TimestampDataGetter = ResultSet::getTimestamp;
    /**
     * Predefined date reader implementation.
     */
    DbDataGetter<LocalDate> DateDataGetter = (resultSet, column) -> {
        var v = resultSet.getDate(column);
        return v == null ? null : v.toLocalDate();
    };
    /**
     * Predefined time reader implementation.
     */
    DbDataGetter<Time> TimeDataGetter = ResultSet::getTime;
    /**
     * Predefined timestamp reader implementation to return as <code>LocalDateTime</code>.
     */
    DbDataGetter<LocalDateTime> DateTimeDataGetter = (resultSet, column) -> {
        var v = resultSet.getTimestamp(column);
        return v == null ? null : v.toLocalDateTime();
    };
    /**
     * Predefined binary array reader implementation.
     */
    DbDataGetter<byte[]> BINARYDataGetter = (resultSet, column) -> {
        try (InputStream inputStream = resultSet.getBinaryStream(column)) {
            return inputStream.readAllBytes();
        }
    };
    /**
     * Predefined big decimal reader implementation.
     */
    DbDataGetter<BigDecimal> DecimalDataGetter = (ResultSet::getBigDecimal);
}
