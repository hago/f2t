/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

/**
 * Additional information for a column type definition.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
public class ColumnTypeModifier {
    private int maxLength = 0;
    private int precision = 0;
    private int scale = 0;
    private String collation;
    private boolean containsNonAscii = false;
    private boolean nullable = true;

    /**
     * Get maximum length of a length limited string type column.
     *
     * @return maximum length
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * Set maximum length for a length limited string type column.
     *
     * @param maxLength maximum length
     */
    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * Get precision(integral part) length of a floating number type column.
     *
     * @return count of precision digits
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Set precision length of a floating number type column.
     *
     * @param precision count of precision digits
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * Get scale(fraction part) length of a floating number type column.
     *
     * @return count of scale digits
     */
    public int getScale() {
        return scale;
    }

    /**
     * Set scale length of a floating number type column.
     *
     * @param scale count of precision digits
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * Get collation settings for a string type column.
     *
     * @return collation name
     */
    public String getCollation() {
        return collation;
    }

    /**
     * Set collation setting for a string type column.
     *
     * @param collation collation name
     */
    public void setCollation(String collation) {
        this.collation = collation;
    }

    /**
     * Whether non ascii character exists in records of a string type column.
     *
     * @return true if non ascii character exists, false if not
     */
    public boolean isContainsNonAscii() {
        return containsNonAscii;
    }

    /**
     * Set the flag of that whether non ascii character exists in records of a string type column.
     *
     * @param containsNonAscii true if non ascii character exists, false if not
     */
    public void setContainsNonAscii(boolean containsNonAscii) {
        this.containsNonAscii = containsNonAscii;
    }

    /**
     * Whether "null" is an acceptable value for this column.
     *
     * @return true for acceptable, otherwise false
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Set the flag to indicate whether "null" is an acceptable value for this column.
     *
     * @param nullable true for acceptable, otherwise false
     */
    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public ColumnTypeModifier() {
        //
    }

    public ColumnTypeModifier(
            int maxLength, int precision, int scale, String collation, boolean containsNonAscii, boolean nullable
    ) {
        this.maxLength = maxLength;
        this.precision = precision;
        this.scale = scale;
        this.collation = collation;
        this.containsNonAscii = containsNonAscii;
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
