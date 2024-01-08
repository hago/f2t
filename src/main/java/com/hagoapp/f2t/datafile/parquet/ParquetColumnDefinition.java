/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet;

import com.hagoapp.f2t.ColumnDefinition;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ParquetColumnDefinition that = (ParquetColumnDefinition) o;

        return Objects.equals(parquetType, that.parquetType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (parquetType != null ? parquetType.hashCode() : 0);
        return result;
    }
}
