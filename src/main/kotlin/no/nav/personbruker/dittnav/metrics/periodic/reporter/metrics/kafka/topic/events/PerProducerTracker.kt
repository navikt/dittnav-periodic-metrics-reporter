package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.EventIdParser
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.FodselsnummerParser
import org.eclipse.collections.impl.set.mutable.UnifiedSet

class PerProducerTracker(initialEntry: UniqueKafkaEventIdentifier) {

    private val userEventIds = UnifiedSet<UserEventIdEntry>()

    private val eventIdFormatCounter = EventIdFormatCounter()

    fun countEventIdsByFormat(): Map<EventIdFormat, Int> = eventIdFormatCounter.formatCount

    fun addEvent(uniqueKafkaEventIdentifier: UniqueKafkaEventIdentifier): Boolean {
        val entry = UserEventIdEntry.fromUniqueIdentifier(uniqueKafkaEventIdentifier)

        return if (userEventIds.add(entry)) {
            eventIdFormatCounter.registerEntry(entry)
            true
        } else {
            false
        }
    }

    init {
        userEventIds.add(UserEventIdEntry.fromUniqueIdentifier(initialEntry))
    }
}

private class EventIdFormatCounter {

    val formatCount = mutableMapOf<EventIdFormat, Int>()

    fun registerEntry(entry: UserEventIdEntry) {
        val format = entry.eventId.getFormat()

        formatCount.compute(format) { _, counter ->
            if (counter == null) {
                1
            } else {
                counter + 1
            }
        }
    }
}

private data class UserEventIdEntry(
        val fodselsnummer: Fodselsnummer,
        val eventId: EventId
) {
    companion object {
        fun fromUniqueIdentifier(uniqueIdentifier: UniqueKafkaEventIdentifier): UserEventIdEntry {
            val fodselsnummer = FodselsnummerParser.parse(uniqueIdentifier.fodselsnummer)

            return if (uniqueIdentifier.fodselsnummer == uniqueIdentifier.eventId && fodselsnummer is FodselsnummerNumeric) {
                UserEventIdEntry(
                        fodselsnummer = fodselsnummer,
                        eventId = EventIdFodselsnummer(uniqueIdentifier.eventId.toLong())
                )
            } else {
                UserEventIdEntry(
                        fodselsnummer = fodselsnummer,
                        eventId = EventIdParser.parseEventId(uniqueIdentifier.eventId)
                )
            }
        }
    }
}
