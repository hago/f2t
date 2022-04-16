/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile.parquet

/**
 * The configuration for a parquet file as target to write.
 *
 * @property namespace namespace of parquet schema
 * @property name  name of parquet schema
 * @property parquetFileName   file name of parquet
 */
data class ParquetWriterConfig(
    val namespace: String?,
    val name: String,
    val parquetFileName: String
)
