package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import java.lang.RuntimeException

object Base32UlidParser {

    fun parseNumericValueFromBase32Ulid(string: String): LongArray {
        if (string.isEmpty()) {
            return LongArray(0)
        }

        val numBits = string.length * 5

        val numBytes = (numBits + 7)  / 8

        val cumulativeValue = LongArray((numBytes + 7) / 8)

        var currentVal = 0L
        var minorIteration = 0
        var majorIteration = 0

        var offset = 0
        var prevOffset: Int
        var bitcount = 0

        for (char in string.reversed()) {
            bitcount += 5
            if (bitcount % 64 < 5) {

                prevOffset = offset
                offset = bitcount % 64
                val offsetInverse = 5 - offset

                val charVal = parseBase32UlidChar(char)

                val highPart = (charVal shr offsetInverse).toLong()
                val lowPart = charVal - (highPart shl offsetInverse)

                currentVal += lowPart * ((32 `to the power of` minorIteration) * (2 `to the power of` prevOffset))

                cumulativeValue[majorIteration] = currentVal
                cumulativeValue[majorIteration + 1] = highPart

                currentVal = highPart
                minorIteration = 0
                majorIteration++
            } else {
                currentVal += parseBase32UlidChar(char) * ((32 `to the power of` minorIteration) * (2 `to the power of` offset))

                minorIteration++
            }
        }

        if (minorIteration > 0) {
            cumulativeValue[majorIteration] = currentVal
        }

        return cumulativeValue
    }

    private infix fun Int.`to the power of`(exponent: Int): Long {
        var cumulative = 1L

        (1..exponent).forEach { _ ->
            cumulative *= this
        }

        return cumulative
    }

    // Account for invalid characters I, L, O and U
    private fun parseBase32UlidChar(char: Char): Int {
        return when (char) {
            in '0'..'9' -> char - '0'
            in 'a'..'h' -> char + 10 - 'a'
            in 'A'..'H' -> char + 10 - 'A'
            in "jk" -> char + 9 - 'a'
            in "JK" -> char + 9 - 'A'
            in "mn" -> char + 8 - 'a'
            in "MN" -> char + 8 - 'A'
            in 'p'..'t' -> char + 7 - 'a'
            in 'P'..'T' -> char + 7 - 'A'
            in 'v'..'z' -> char + 6 - 'a'
            in 'V'..'Z' -> char + 6 - 'A'
            else -> throw RuntimeException("Kan ikke parse char $char for base-32 (ULID).")
        }
    }
}