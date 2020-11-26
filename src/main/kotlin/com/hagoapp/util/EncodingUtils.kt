/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.util

import org.mozilla.universalchardet.UniversalDetector
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

class EncodingUtils {
    companion object {

        fun guessEncoding(stream: InputStream): String {
            val det = UniversalDetector(null)
            ByteArrayOutputStream().use {
                val buffer = ByteArray(1024 * 1024)
                while (true) {
                    val i = stream.read(buffer, 0, buffer.size)
                    if (i == -1) {
                        det.dataEnd()
                        break
                    } else if (i == 0) {
                        continue
                    } else {
                        det.handleData(buffer, 0, i)
                        if (det.isDone) {
                            break
                        }
                    }
                }
            }
            //println(det.detectedCharset)
            return normalizeEncoding(det.detectedCharset)
        }

        fun guessEncoding(file: File): String {
            FileInputStream(file).use {
                return guessEncoding(it)
            }
        }

        fun guessEncoding(fileName: String): String {
            FileInputStream(fileName).use {
                return guessEncoding(it)
            }
        }

        fun normalizeEncoding(enc: String?): String {
            val allCharSets = Charset.availableCharsets().keys
            return when {
                enc == null -> StandardCharsets.UTF_8.displayName()
                allCharSets.any { it.compareTo(enc, true) == 0 } ->
                    mapDetectorEnc2JavaEnc(enc, StandardCharsets.UTF_8.displayName())!!
                else -> StandardCharsets.UTF_8.displayName()
            }
        }

        fun mapDetectorEnc2JavaEnc(enc: String): String? {
            return mapDetectorEnc2JavaEnc(enc, null)
        }

        fun mapDetectorEnc2JavaEnc(enc: String, default: String?): String? {
            return when (val enc0 = enc.toUpperCase()) {
                "ISO-2022-JP",
                "ISO-2022-CN",
                "ISO-2022-KR",
                "ISO-8859-5",
                "ISO-8859-7",
                "ISO-8859-8",
                "BIG5",
                "GB18030",
                "EUC-JP",
                "EUC-KR",
                "Shift_JIS",
                "IBM855",
                "IBM866",
                "KOI8-R",
                "WINDOWS-1251",
                "WINDOWS-1252",
                "WINDOWS-1253",
                "WINDOWS-1255",
                "UTF-8",
                "UTF-16BE",
                "UTF-16LE",
                "UTF-32BE",
                "UTF-32LE" -> enc0
                "BIG-5" -> "Big5"
                "EUC-TW" -> "x-EUC-TW"
                "HZ-GB-2312" -> "GB18030"
                //"X-ISO-10646-UCS-4-3412",
                //"X-ISO-10646-UCS-4-2143",
                "MACCYRILLIC" -> "x-MacCyrillic"
                "SHIFT_JIS" -> "Shift_JIS"
                else -> default
            }
        }
    }

}