package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier

class PerProducerTracker(initialEntry: UniqueKafkaEventIdentifier) {

    private val userEventIds = HashSet<UserEventIdEntry>()

    fun addEvent(uniqueKafkaEventIdentifier: UniqueKafkaEventIdentifier): Boolean {
        return userEventIds.add(UserEventIdEntry.fromUniqueIdentifier(uniqueKafkaEventIdentifier))
    }

    init {
        userEventIds.add(UserEventIdEntry.fromUniqueIdentifier(initialEntry))
    }
}

private data class UserEventIdEntry(
        val fodselsnummer: Fodselsnummer,
        val eventId: String
) {
    companion object {
        fun fromUniqueIdentifier(uniqueIdentifier: UniqueKafkaEventIdentifier) =
                UserEventIdEntry(Fodselsnummer.fromString(uniqueIdentifier.fodselsnummer), uniqueIdentifier.eventId)
    }
}

private interface Fodselsnummer {
    companion object {

        fun fromString(fodselsnummerString: String): Fodselsnummer {
            val longValue = fodselsnummerString.toLongOrNull()

            return if (longValue != null) {
                FodselsnummerNumeric(longValue)
            } else {
                FodselsnummerString(fodselsnummerString)
            }
        }

    }
}

private data class FodselsnummerString(val stringValue: String): Fodselsnummer

private data class FodselsnummerNumeric(val longValue: Long): Fodselsnummer