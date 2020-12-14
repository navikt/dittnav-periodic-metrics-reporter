package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.Base16Parser.parseNumericValueFromBase16
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.Base32UlidParser.parseNumericValueFromBase32Ulid

object EventIdParser {
    private const val BASE_16 = "[0-9a-fA-F]"

    private const val BASE_32_ULID = "[0-9ABCDEFGHJKMNPQRSTVWXYZabcdefghjkmnpqrstvwxyz]"

    private val UUID_PATTERN = "^$BASE_16{8}-$BASE_16{4}-$BASE_16{4}-$BASE_16{4}-$BASE_16{12}$".toRegex()

    // We know some eventids look like normal uuids prefixed with some character. Handle these separately
    private val CUSTOM_UUID_PATTERN = "^([a-zA-Z])($BASE_16{8}-$BASE_16{4}-$BASE_16{4}-$BASE_16{4}-$BASE_16{12}$)".toRegex()

    private val ULID_PATTERN = "^[0-7]$BASE_32_ULID{25}$".toRegex()

    fun parseEventId(eventIdString: String): EventId {
        return when {
            UUID_PATTERN.matches(eventIdString) -> parseUuid(eventIdString)
            ULID_PATTERN.matches(eventIdString) -> parseUlid(eventIdString)
            CUSTOM_UUID_PATTERN.matches(eventIdString) -> parseCustomUuid(eventIdString)
            else -> EventIdString(stringValue = eventIdString)
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
        return CUSTOM_UUID_PATTERN.find(eventIdString)!!.destructured.let { (prefix, uuidString) ->
            val withoutHyphen = uuidString.replace("-", "")

            val dataAs128BitNumber = parseNumericValueFromBase16(withoutHyphen)

            EventIdCustomUuid(
                    prefix = prefix.first(),
                    lowBits = dataAs128BitNumber[0],
                    highBits = dataAs128BitNumber[1]
            )
        }
    }
}