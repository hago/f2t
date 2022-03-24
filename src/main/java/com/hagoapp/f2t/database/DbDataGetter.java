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

public interface DbDataGetter<T> {
    T getTypedValue(ResultSet resultSet, String column) throws SQLException, IOException;

    DbDataGetter<String> StringDataGetter = ResultSet::getString;
    DbDataGetter<String> NStringDataGetter = ResultSet::getNString;
    DbDataGetter<Integer> IntDataGetter = ResultSet::getInt;
    DbDataGetter<Byte> TinyIntDataGetter = ResultSet::getByte;
    DbDataGetter<Short> ShortDataGetter = ResultSet::getShort;
    DbDataGetter<Long> LongDataGetter = ResultSet::getLong;
    DbDataGetter<Float> FloatDataGetter = ResultSet::getFloat;
    DbDataGetter<Double> DoubleDataGetter = ResultSet::getDouble;
    DbDataGetter<Boolean> BooleanDataGetter = ResultSet::getBoolean;
    DbDataGetter<Timestamp> TimestampDataGetter = ResultSet::getTimestamp;
    DbDataGetter<LocalDate> DateDataGetter = (resultSet, column) -> {
        var v = resultSet.getDate(column);
        return v == null ? null : v.toLocalDate();
    };
    DbDataGetter<Time> TimeDataGetter = ResultSet::getTime;
    DbDataGetter<LocalDateTime> DateTimeDataGetter = (resultSet, column) -> {
        var v = resultSet.getTimestamp(column);
        return v == null ? null : v.toLocalDateTime();
    };
    DbDataGetter<byte[]> BINARYDataGetter = (resultSet, column) -> {
        try (InputStream inputStream = resultSet.getBinaryStream(column)) {
            return inputStream.readAllBytes();
        }
    };
    DbDataGetter<BigDecimal> DecimalDataGetter = (ResultSet::getBigDecimal);
}
