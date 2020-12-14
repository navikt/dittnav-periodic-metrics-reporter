package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import java.lang.RuntimeException

object Base16Parser {

    fun parseNumericValueFromBase16(string: String): LongArray {

        if (string.isEmpty()) {
            return LongArray(0)
        }

        // In base-16, each character encodes 4 bits
        val numBits = string.length * 4

        // Bytes needed is number of bits divided 8, rounded up
        val numBytes = (numBits + 7)  / 8

        // Each Long is 8 bytes wide. Thus number of longs needed is also number of bytes divided by 8, rounded up
        val cumulativeValue = LongArray((numBytes + 7) / 8)

        var currentVal = 0L
        var minorIteration = 0
        var majorIteration = 0

        for (char in string.reversed()) {
            currentVal += parseBase16Char(char) * (16 `to the power of` minorIteration)

            minorIteration++

            // Our current long value is saturated when we have handled 16 characters. Thus we need to store our
            // current result and prepare for calculating our next long value
            if (minorIteration == 16) {
                cumulativeValue[majorIteration] = currentVal

                currentVal = 0
                minorIteration = 0
                majorIteration++
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

    private fun parseBase16Char(char: Char): Int {
        return when (char) {
            in '0'..'9' -> char - '0'
            in 'a'..'f' -> char + 10 - 'a'
            in 'A'..'F' -> char + 10 - 'A'
            else -> throw RuntimeException("Kan ikke parse char $char. for base-16")
        }
    }
}