package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.EventIdParser

class PerProducerTracker(initialEntry: UniqueKafkaEventIdentifier, expectedEventsPerUserPerProducer: Int) {

    private val userEventIds = HashSet<UserEventIdEntry>(expectedEventsPerUserPerProducer)

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
                        Fodselsnummer.fromString(uniqueIdentifier.fodselsnummer),
                        EventIdParser.parseEventId(uniqueIdentifier.eventId)
                )
    }
}