/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

/**
 * Additional information for a column type definition
 *
 * @author suncjs
 */
public class ColumnTypeModifier {
    private int maxLength = 0;
    private int precision = 0;
    private int scale = 0;
    private String collation;
    private boolean hasNonAsciiChar = false;
    private boolean nullable = true;

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public boolean isHasNonAsciiChar() {
        return hasNonAsciiChar;
    }

    public void setHasNonAsciiChar(boolean hasNonAsciiChar) {
        this.hasNonAsciiChar = hasNonAsciiChar;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    @Override
    public String toString() {
        return "ColumnTypeModifier{" +
                "maxLength=" + maxLength +
                ", precision=" + precision +
                ", scale=" + scale +
                ", nullable=" + nullable +
                ", collation='" + collation + '\'' +
                '}';
    }
}
