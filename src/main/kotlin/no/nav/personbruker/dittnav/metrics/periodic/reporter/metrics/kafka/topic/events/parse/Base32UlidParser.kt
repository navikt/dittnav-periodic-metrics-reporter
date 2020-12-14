package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import java.lang.RuntimeException

object Base32UlidParser {

    fun parseNumericValueFromBase32Ulid(string: String): LongArray {
        if (string.isEmpty()) {
            return LongArray(0)
        }

        // In base-32, each character encodes 5 bits
        val numBits = string.length * 5

        // Bytes needed is number of bits divided 8, rounded up
        val numBytes = (numBits + 7)  / 8

        // Each Long is 8 bytes wide. Thus number of longs needed is also number of bytes divided by 8, rounded up
        val cumulativeValue = LongArray((numBytes + 7) / 8)

        var currentVal = 0L
        var minorIteration = 0
        var majorIteration = 0

        var offset = 0
        var prevOffset: Int
        var bitcount = 0

        for (char in string.reversed()) {
            bitcount += 5
            // Handle special case when current long value is about to be completely saturated
            if (bitcount % 64 < 5) {

                // Offset determines the cut-off point at which the lower bits are placed in current long variable,
                // and higher bits are placed in next long variable.
                prevOffset = offset
                offset = bitcount % 64

                // Inverse offset is the position of the offset as counted from the left, rather than the right
                val offsetInverse = 5 - offset

                val charVal = parseBase32UlidChar(char)

                // Let's say our offset is 3, and inverse 2. This makes the bit pattern hhlll. Meaning that the
                // 3 rightmost bits would be placed at the left end of our current long value, while the 2 leftmost
                // bits would be placed at the right end of our next long.
                val highPart = (charVal shr offsetInverse).toLong()
                val lowPart = charVal - (highPart shl offsetInverse)

                currentVal += lowPart * ((32 `to the power of` minorIteration) * (2 `to the power of` prevOffset))

                // Store current and next longs in return array
                cumulativeValue[majorIteration] = currentVal
                cumulativeValue[majorIteration + 1] = highPart

                // Set current long to next long
                currentVal = highPart

                // Reset minorIteration and increment majorIteration to reflect starting at the beginning of our next long
                minorIteration = 0
                majorIteration++
            } else {
                // Because we have to consider the offset as well as the position of the character, we also have to
                // multiply the value by 1, 2, 4, 8, or 16, based on our current offset.
                currentVal += parseBase32UlidChar(char) * ((32 `to the power of` minorIteration) * (2 `to the power of` offset))

                // Increment minorIteration to reflect moving one step left in our current long
                minorIteration++
            }
        }

        // Store current updated value in return array
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