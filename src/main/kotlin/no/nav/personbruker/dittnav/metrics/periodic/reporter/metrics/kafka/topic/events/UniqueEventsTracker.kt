package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier

class UniqueEventsTracker {

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
            perProducerMap[eventIdentifier.systembruker] = PerProducerTracker(eventIdentifier)
            uniqueEventsVar++
            true
        }
    }
}