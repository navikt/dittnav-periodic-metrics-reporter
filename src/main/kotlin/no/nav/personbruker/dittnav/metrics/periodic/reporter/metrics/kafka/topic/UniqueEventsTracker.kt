package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier

class UniqueEventsTracker(private val expectedEventsPerUserPerProducer: Int) {

    private val perProducerMap = HashMap<String, PerProducerTracker>()

    private var uniqueEventsVar: Int = 0

    val uniqueEvents: Int get() = uniqueEventsVar

    fun addEvent(eventIdentifier: UniqueKafkaEventIdentifier): Boolean {
        return if (perProducerMap.containsKey(eventIdentifier.systembruker)) {
            val isUnique = perProducerMap[eventIdentifier.systembruker]!!.addEvent(eventIdentifier)

            if (isUnique) {
                uniqueEventsVar++
            }

            isUnique
        } else {
            perProducerMap[eventIdentifier.systembruker] = PerProducerTracker(eventIdentifier, expectedEventsPerUserPerProducer)
            uniqueEventsVar++
            true
        }
    }
}

private class PerProducerTracker(initialEntry: UniqueKafkaEventIdentifier, expectedEventsPerUserPerProducer: Int) {

    private val userEventIds = HashSet<UserEventIdEntry>(expectedEventsPerUserPerProducer)

    fun addEvent(uniqueKafkaEventIdentifier: UniqueKafkaEventIdentifier): Boolean {
        return userEventIds.add(UserEventIdEntry(uniqueKafkaEventIdentifier.fodselsnummer, uniqueKafkaEventIdentifier.eventId))
    }

    init {
        userEventIds.add(UserEventIdEntry(initialEntry.fodselsnummer, initialEntry.eventId))
    }
}

private data class UserEventIdEntry(
        val fodselsnummer: String,
        val eventId: String
)