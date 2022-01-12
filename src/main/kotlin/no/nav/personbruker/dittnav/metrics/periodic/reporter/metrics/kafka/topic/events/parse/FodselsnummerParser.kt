package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.*

// Denne "parseren" drar nytte av det faktum at de to siste siffrene i et fødselsnummer er gitt
// etter de ni første. Dette betyr at hvis vi antar at alle fødselsnummer vi mottar er gyldige,
// vil de to siste siffrene kun gi oss informasjon vi allerede har. Vi kan derfor sløyfe disse siffrene
// uten å tape data om unikhet, og vi kan plassere all nødvendig info i en Int.
// Dette sparer oss fire byte per event, sett mot å plassere hele sifferet i en Long.
object FodselsnummerParser {
    private val FODSELSNUMMER_FORMAT = "^[0-9]{11}$".toRegex()

    fun parse(fnrString: String): Fodselsnummer {

        return when {
            FODSELSNUMMER_FORMAT.matches(fnrString) -> parseAsFnr(fnrString)
            else -> FodselsnummerPlainText(fnrString)
        }
    }

    private fun parseAsFnr(fnrString: String): FodselsnummerNumeric {

        val dataDigits = fnrString.withoutControlDigits

        return FodselsnummerNumeric(dataDigits.toInt())
    }

    private val String.withoutControlDigits: String get() = subSequence(0, 9).toString()


}

