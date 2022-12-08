/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet;

import com.hagoapp.f2t.ColumnDefinition;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Definition for a column from parquet.
 *
 * @author suncjs
 * @since 0.7.5
 */
public class ParquetColumnDefinition extends ColumnDefinition {
    private PrimitiveType parquetType;

    public PrimitiveType getParquetType() {
        return parquetType;
    }

    public void setParquetType(PrimitiveType parquetType) {
        this.parquetType = parquetType;
    }

    @Override
    public String toString() {
        return "ParquetColumnDefinition{" +
                "name='" + getName() + '\'' +
                ", dataType=" + getDataType() +
                ", databaseType=" + getDatabaseTypeName() +
                ", typeModifier=" + getTypeModifier() +
                "parquetType=" + parquetType +
                '}';
    }
}
