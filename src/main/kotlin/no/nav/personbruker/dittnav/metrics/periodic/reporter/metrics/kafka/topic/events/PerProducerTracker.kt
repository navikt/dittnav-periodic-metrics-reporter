package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier

class PerProducerTracker(
        initialEntry: UniqueKafkaEventIdentifier,
        private val expectedEventsPerUserPerProducer: Int
) {

    private val eventIdsPerUser = HashMap<Fodselsnummer, HashSet<String>>()

    fun addEvent(uniqueKafkaEventIdentifier: UniqueKafkaEventIdentifier): Boolean {
        val fodselsnummer = Fodselsnummer.fromString(uniqueKafkaEventIdentifier.fodselsnummer)

        return if (eventIdsPerUser.containsKey(fodselsnummer)) {
            eventIdsPerUser[fodselsnummer]!!.add(uniqueKafkaEventIdentifier.eventId)
        } else {
            eventIdsPerUser[fodselsnummer] = HashSet(expectedEventsPerUserPerProducer)
            eventIdsPerUser[fodselsnummer]!!.add(uniqueKafkaEventIdentifier.eventId)
            true
        }
    }

    init {
        addEvent(initialEntry)
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