/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.HashSet;
import java.util.Set;

/**
 * Definition of a column from data file.
 *
 * @author Chaojun Sun
 * @since 0.6
 */
public class FileColumnDefinition extends ColumnDefinition {

    public FileColumnDefinition() {
        super();
    }

    public FileColumnDefinition(String name) {
        super(name);
    }

    public FileColumnDefinition(String name, int order) {
        super(name);
        this.order = order;
    }

    public FileColumnDefinition(String name, Set<JDBCType> possibleTypes) {
        super(name);
        this.possibleTypes = possibleTypes;
    }

    public FileColumnDefinition(String name, Set<JDBCType> possibleTypes, JDBCType candidate0) {
        super(name, candidate0);
        this.possibleTypes = possibleTypes;
    }

    public FileColumnDefinition(
            String name, Set<JDBCType> possibleTypes,
            JDBCType candidate0, BigDecimal minimum, BigDecimal maximum, boolean containsEmpty
    ) {
        super(name, candidate0);
        this.possibleTypes = possibleTypes;
        this.minimum = minimum;
        this.maximum = maximum;
        this.containsEmpty = containsEmpty;
    }

    private Set<JDBCType> possibleTypes = new HashSet<>();
    private int order;
    private BigDecimal minimum;
    private BigDecimal maximum;
    private boolean containsEmpty = false;

    /**
     * Get the 0 indexed order number of the column from file.
     *
     * @return order number
     */
    public int getOrder() {
        return order;
    }

    /**
     * Set the 0 indexed order number of the column from file.
     *
     * @param order order number
     */
    public void setOrder(int order) {
        this.order = order;
    }

    /**
     * Get the minimum value of this numeric column from file.
     *
     * @return minimum value of numeric column
     */
    public BigDecimal getMinimum() {
        return minimum;
    }

    /**
     * Set the minimum value of this numeric column from file.
     *
     * @param minimum minimum value of numeric column
     */
    public void setMinimum(BigDecimal minimum) {
        this.minimum = minimum;
    }

    /**
     * Get the maximum value of this numeric column from file.
     *
     * @return maximum value of numeric column
     */
    public BigDecimal getMaximum() {
        return maximum;
    }

    /**
     * Set the maximum value of this numeric column from file.
     *
     * @param maximum maximum value of numeric column
     */
    public void setMaximum(BigDecimal maximum) {
        this.maximum = maximum;
    }

    /**
     * Get whether empty value exists in this text column from file.
     *
     * @return true if exists, otherwise false
     */
    public boolean isContainsEmpty() {
        return containsEmpty;
    }

    /**
     * Set whether empty value exists in this text column from file.
     *
     * @param containsEmpty true if exists, otherwise false
     */
    public void setContainsEmpty(boolean containsEmpty) {
        this.containsEmpty = containsEmpty;
    }

    /**
     * Get all possible data types those can store values from this file column,
     *
     * @return all possible data types
     */
    public Set<JDBCType> getPossibleTypes() {
        return possibleTypes;
    }

    /**
     * Set all possible data types those can store values from this file column,
     *
     * @param possibleTypes all possible data types
     */
    public void setPossibleTypes(Set<JDBCType> possibleTypes) {
        this.possibleTypes = possibleTypes;
    }

    @Override
    public String toString() {
        return "FileColumnDefinition{" +
                "possibleTypes=" + possibleTypes +
                ", order=" + order +
                ", minimum=" + minimum +
                ", maximum=" + maximum +
                ", containsEmpty=" + containsEmpty +
                ", parent={" + super.toString() +
                "}}";
    }
}
