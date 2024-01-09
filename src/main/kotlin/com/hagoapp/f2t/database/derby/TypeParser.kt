/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.database.derby

import com.hagoapp.f2t.ColumnTypeModifier
import org.slf4j.LoggerFactory
import java.lang.reflect.Modifier
import java.sql.JDBCType

/**
 * A helper class to parse data type string from Derby's metadata system table to data types.
 *
 * @author suncjs
 * @since 0.8.5
 */
class TypeParser private constructor() {
    interface Parser {
        fun supportedPrefix(): String
        fun supportedSuffix(): String? {
            return null
        }

        fun parse(dataTypeString: String): Pair<JDBCType, ColumnTypeModifier>
    }

    companion object {

        // prefix -> (suffix, parseAbleType), suffix could be null for no check.
        private val parsers = Parsers::class.java.fields.map { f ->
            val modifier = f.modifiers
            if (Modifier.isStatic(modifier) && Modifier.isPublic(modifier)
                && f.type.isAssignableFrom(Parser::class.java)
            ) {
                val parser = f[null] as Parser
                Triple(parser.supportedPrefix(), parser.supportedSuffix(), parser)
            } else {
                null
            }
        }.filterNotNull()
        private val logger = LoggerFactory.getLogger(TypeParser::class.java)

        @JvmStatic
        fun parseType(colDataType: String): Pair<JDBCType, ColumnTypeModifier> {
            val parser = parsers.firstOrNull { p ->
                colDataType.startsWith(p.first) &&
                        ((p.second == null) || (colDataType.endsWith(p.second!!)))
            }
            return if (parser == null) {
                logger.warn("Parser for Derby's type {} not found, CLOB used", colDataType)
                Parsers.clobParser.parse(colDataType)
            } else parser.third.parse(colDataType)
        }
    }
}
