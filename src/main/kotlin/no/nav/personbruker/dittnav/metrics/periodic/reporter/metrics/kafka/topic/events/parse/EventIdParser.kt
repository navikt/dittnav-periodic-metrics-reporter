package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.Base16Parser.parseNumericValueFromBase16
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.Base32UlidParser.parseNumericValueFromBase32Ulid

// Hensikten med denne parseren er å 'komprimere' eventId-er ved å konvertere dem til numeriske verdier.
// Dette er mulig fordi vi ser at de aller fleste eventId-er bruker en form for UUID (og på sikt ULID).
// UUID-er og ULID-er er 128-bits data representert i string-format i henholdsvis Base-16 og Base-32.
// Dette betyr at UUID-er og ULID-er fritt kan konverteres 1:1 fram og tilbake til andre 128-bits dataformat
// uten å tape data. Per i dag finnes det native 128-bits primitiver i Java, men dersom vi håndterer den ekstra
// logikken selv, kan vi i stedet bruke to 64-bits Long verdier.
//
// Grunnen til at dette bruker mindre minne er at vi har mindre ubrukte bits. For å illustrere effekten av dette
// kan man regne på antal bytes brukt for å holde på samme informasjon før og etter 'komprimering' (Dette er
// noe forenklet, og tar ikke med overhead per String objekt):
//
// UUID: 36 heksadesimaletegn + 4 bindestreker = 40 tegn. 2 bytes per bokstav i java -> 40 * 2 = 80 bytes
// ULID: 26 tegn i base-32. 2 bytes per bokstav i java -> 26 * 2 = 52 bytes
// 2 Longs: 8 bytes per Long i java -> 8 * 2 = 16 bytes
//
// Vi sparer dermed minst 64 (80 - 16) eller 36 (52 - 16) bytes per eventId som lar seg konvertere på denne måten.
object EventIdParser {
    private const val BASE_16 = "[0-9a-fA-F]"

    // Base-32 as used in ULIDs use 22 characters in the range A-Z, omitting I, L, O and U
    private const val BASE_32_ULID = "[0-9ABCDEFGHJKMNPQRSTVWXYZabcdefghjkmnpqrstvwxyz]"

    private val UUID_PATTERN = "^$BASE_16{8}-$BASE_16{4}-$BASE_16{4}-$BASE_16{4}-$BASE_16{12}$".toRegex()

    // We know some eventids look like normal uuids prefixed with some character. Handle these separately
    private val PREFIXED_UUID_PATTERN = "^([a-zA-Z])($BASE_16{8}-$BASE_16{4}-$BASE_16{4}-$BASE_16{4}-$BASE_16{12}$)".toRegex()

    // ULIDs are a total of 128 bits wide, with 5 bits encoded per character. This means that the most leftmost
    // character is only 3 bits wide (128 mod 5), and can only be in the range 0-7 inclusive.
    private val ULID_PATTERN = "^[0-7]$BASE_32_ULID{25}$".toRegex()

    fun parseEventId(eventIdString: String): EventId {
        return when {
            UUID_PATTERN.matches(eventIdString) -> parseUuid(eventIdString)
            ULID_PATTERN.matches(eventIdString) -> parseUlid(eventIdString)
            PREFIXED_UUID_PATTERN.matches(eventIdString) -> parseCustomUuid(eventIdString)
            else -> EventIdPlainText(stringValue = eventIdString)
        }
    }

    private fun parseUuid(uuidString: String): EventId {
        val withoutHyphen = uuidString.replace("-", "")

        val dataAs128BitNumber = parseNumericValueFromBase16(withoutHyphen)

        return EventIdUuid(lowBits = dataAs128BitNumber[0], highBits = dataAs128BitNumber[1])
    }

    private fun parseUlid(ulidString: String): EventId {
        val dataAs128BitNumber = parseNumericValueFromBase32Ulid(ulidString)

        return EventIdUlid(lowBits = dataAs128BitNumber[0], highBits = dataAs128BitNumber[1])
    }

    private fun parseCustomUuid(eventIdString: String): EventId {
        return PREFIXED_UUID_PATTERN.find(eventIdString)!!.destructured.let { (prefix, uuidString) ->
            val withoutHyphen = uuidString.replace("-", "")

            val dataAs128BitNumber = parseNumericValueFromBase16(withoutHyphen)

            EventIdPrefixedUuid(
                    prefix = prefix.first(),
                    lowBits = dataAs128BitNumber[0],
                    highBits = dataAs128BitNumber[1]
            )
        }
    }
}