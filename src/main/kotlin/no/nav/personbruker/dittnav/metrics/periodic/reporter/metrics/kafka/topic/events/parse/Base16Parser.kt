package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import java.lang.RuntimeException

object Base16Parser {
    fun parseNumericValueFromBase16(string: String): LongArray {

        if (string.isEmpty()) {
            return LongArray(0)
        }

        val numBits = string.length * 4

        val numBytes = (numBits + 7)  / 8

        val cumulativeValue = LongArray((numBytes + 7) / 8)

        var currentVal = 0L
        var minorIteration = 0
        var majorIteration = 0

        for (char in string.reversed()) {
            currentVal += parseBase16Char(char) * (16 `to the power of` minorIteration)

            minorIteration++


            if (minorIteration == 16) {
                cumulativeValue[majorIteration] = currentVal

                currentVal = 0
                minorIteration = 0
                majorIteration++
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

    private fun parseBase16Char(char: Char): Int {
        return when (char) {
            in '0'..'9' -> char - '0'
            in 'a'..'f' -> char + 10 - 'a'
            in 'A'..'F' -> char + 10 - 'A'
            else -> throw RuntimeException("Kan ikke parse char $char. for base-16")
        }
    }
}