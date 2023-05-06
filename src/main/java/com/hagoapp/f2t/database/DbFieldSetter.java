/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database;

import org.jetbrains.annotations.NotNull;

import java.sql.PreparedStatement;

public abstract class DbFieldSetter {

    protected DataTransformer transformer = DataTransformer.defaultTransformer;

    public abstract void set(@NotNull PreparedStatement stmt, int i, Object value);

    public DbFieldSetter withTransformer(DataTransformer transformer) {
        this.transformer = transformer;
        return this;
    }

}
