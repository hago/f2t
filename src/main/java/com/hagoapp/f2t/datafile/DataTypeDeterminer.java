/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import java.sql.JDBCType;
import java.util.Set;

public interface DataTypeDeterminer {
    JDBCType determineTypes(Set<JDBCType> types);
}
