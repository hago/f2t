/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.util

import org.mozilla.universalchardet.UniversalDetector
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.Instant
import javax.xml.bind.annotation.XmlType.DEFAULT

/**
 * A utility class to deal with encodings.
 *
 * @author Chaojun Sun
 * @since 0.2
 */
class EncodingUtils {
    companion object {

        private val logger = LoggerFactory.getLogger(EncodingUtils::class.java)

        /**
         * This method will try to figure out which character set the input stream is using.
         *
         * @param stream input stream which should be text
         * @return the name of character set being used
         */
        @JvmStatic
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
            logger.debug("{} detected", det.detectedCharset)
            return normalizeEncoding(det.detectedCharset)
        }

        /**
         * This method will try to figure out which character set the file is using.
         *
         * @param file input file object which should be text
         * @return the name of character set being used
         */
        @JvmStatic
        fun guessEncoding(file: File): String {
            FileInputStream(file).use {
                return guessEncoding(it)
            }
        }

        /**
         * This method will try to figure out which character set the file is using.
         *
         * @param fileName input file name which should be text
         * @return the name of character set being used
         */
        @JvmStatic
        fun guessEncoding(fileName: String): String {
            FileInputStream(fileName).use {
                return guessEncoding(it)
            }
        }

        /**
         * This method will try to convert input encoding name to a standard encoding name defined in Java.
         *
         * @param enc   input encoding name
         * @return Java encoding name, or UTF-8 if attempts for input are failed
         */
        @JvmStatic
        fun normalizeEncoding(enc: String?): String {
            val allCharSets = Charset.availableCharsets().keys
            return when {
                enc == null -> StandardCharsets.UTF_8.displayName()
                allCharSets.any { it.compareTo(enc, true) == 0 } ->
                    mapDetectorEnc2JavaEnc(enc, StandardCharsets.UTF_8.displayName())!!

                else -> StandardCharsets.UTF_8.displayName()
            }
        }

        /**
         * This method will try to map encoding names from mozilla <code>universalchardet</code> library to standard
         * Java encoding name, UTF-8 is returned when mapping failed.
         *
         * @param enc   mozilla encoding name
         * @return Java encoding name
         */
        @JvmStatic
        fun mapDetectorEnc2JavaEnc(enc: String): String? {
            return mapDetectorEnc2JavaEnc(enc, null)
        }

        /**
         * This method will try to map encoding names from mozilla <code>universalchardet</code> library to standard
         * Java encoding name, a failsafe default value is returned when mapping failed.
         *
         * @param enc   mozilla encoding name
         * @param default   failsafe encoding name
         * @return Java encoding name
         */
        @JvmStatic
        fun mapDetectorEnc2JavaEnc(enc: String, default: String?): String? {
            return when (val enc0 = enc.uppercase()) {
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

        /**
         * This method will check whether input text contains any non-ascii character.
         *
         * @param s input text
         * @return true if none of any non-ascii character exists, otherwise false
         */
        @JvmStatic
        fun isAsciiText(s: String?): Boolean {
            if (s == null) {
                return true
            }
            return try {
                val encoder = StandardCharsets.US_ASCII.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                encoder.canEncode(s)
            } catch (e: CharacterCodingException) {
                false
            }
        }

        const val DEFAULT_CANDIDATE_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        const val LETTERS_ONLY_CANDIDATE_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

        @JvmStatic
        @JvmOverloads
        fun createRandomString(size: Int, candidateChars: String = DEFAULT_CANDIDATE_CHARS): String {
            val seed = Instant.now().toEpochMilli()
            val random = SecureRandom(ByteArray(8) { i -> seed.shr(i).and(0x000000ff).toByte() })
            return String(CharArray(size) { _ ->
                candidateChars[random.nextInt(candidateChars.length)]
            })
        }
    }

}
