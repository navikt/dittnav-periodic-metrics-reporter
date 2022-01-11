package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.*

object FodselsnummerParser {
    private val FODSELSNUMMER_FORMAT = "^[0-3][0-9][0-1][0-9]{8}$".toRegex()
    private val D_NUMMER_FORMAT = "^[4-7][0-9][0-1][0-9]{8}$".toRegex()
    private val H_NUMMER_FORMAT = "^[0-3][0-9][4-5][0-9]{8}$".toRegex()

    fun parse(fnrString: String): Fodselsnummer {

        return when {
            FODSELSNUMMER_FORMAT.matches(fnrString) -> parseAsFnr(fnrString)
            D_NUMMER_FORMAT.matches(fnrString) -> parseAsDnr(fnrString)
            H_NUMMER_FORMAT.matches(fnrString) -> parseAsHnr(fnrString)
            else -> FodselsnummerPlainText(fnrString)
        }
    }

    private fun parseAsFnr(fnrString: String): FodselsnummerNumeric {
        val encoded = buildString(9) {
            append(fnrString.month)
            append(fnrString.day)
            append(fnrString.year)
            append(fnrString.id)
        }

        val backingValue = encoded.toInt()

        return FodselsnummerNumeric(backingValue)
    }

    private fun parseAsDnr(fnrString: String): FodselsnummerDNummer {
        val encoded = buildString(9) {
            append(fnrString.month)
            append(fnrString.day)
            append(fnrString.year)
            append(fnrString.id)
        }

        val backingValue = encoded.toInt()

        return FodselsnummerDNummer(backingValue)
    }

    private fun parseAsHnr(fnrString: String): FodselsnummerHNummer {

        val encoded = buildString(9) {
            append(calculateMonthForHnr(fnrString.month))
            append(fnrString.day)
            append(fnrString.year)
            append(fnrString.id)
        }

        val backingValue = encoded.toInt()

        return FodselsnummerHNummer(backingValue)
    }

    private fun calculateMonthForHnr(monthSegment: CharSequence): String {
        val offsetMonthAsInt = monthSegment.toString().toInt()

        val trueMonthAsInt = offsetMonthAsInt - 40

        return trueMonthAsInt.toString()
    }

    private val String.day: CharSequence get() = subSequence(0, 2)
    private val String.month: CharSequence get() = subSequence(2, 4)
    private val String.year: CharSequence get() = subSequence(4, 6)
    private val String.id: CharSequence get() = subSequence(6, 9)


}

