package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.EventIdParser

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
        val eventId: EventId
) {
    companion object {
        fun fromUniqueIdentifier(uniqueIdentifier: UniqueKafkaEventIdentifier) =
                UserEventIdEntry(
                        fodselsnummer = Fodselsnummer.fromString(uniqueIdentifier.fodselsnummer),
                        eventId = EventIdParser.parseEventId(uniqueIdentifier.eventId)
                )
    }
}