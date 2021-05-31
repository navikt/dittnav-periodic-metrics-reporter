package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions
import org.slf4j.LoggerFactory

class SessionComparator(
    val topic: CountingMetricsSessions,
    val database: CountingMetricsSessions
) {

    private val log = LoggerFactory.getLogger(SessionComparator::class.java)

    private val eventTypesInBothSources = mutableListOf<EventType>()
    private val internalEventTypes = listOf(EventType.BESKJED_INTERN, EventType.INNBOKS_INTERN, EventType.STATUSOPPDATERING_INTERN, EventType.DONE_INTERN, EventType.OPPGAVE_INTERN, EventType.FEILRESPONS)

    init {
        EventType.values().forEach { eventType ->
            if (isPresentInBothSources(eventType)) {
                eventTypesInBothSources.add(eventType)
            } else {
                logWarningWithInfoAboutWhatSourcesWasMissingTheEventType(eventType)
            }
        }
    }

    private fun isPresentInBothSources(eventType: EventType): Boolean {
        return if(internalEventTypes.contains(eventType)) {
            topic.getEventTypesWithSession().contains(eventType)
        } else {
            topic.getEventTypesWithSession().contains(eventType) && database.getEventTypesWithSession().contains(eventType)
        }
    }

    private fun logWarningWithInfoAboutWhatSourcesWasMissingTheEventType(eventType: EventType) {
        when {
            topic.getEventTypesWithSession().contains(eventType) -> {
                val numberOfEvents = topic.getForType(eventType).getNumberOfUniqueEvents()
                log.warn("Eventtypen '$eventType' ble kun telt for topic, og ikke i databasen. Fant $numberOfEvents eventer.")

            }
            database.getEventTypesWithSession().contains(eventType) -> {
                val numberOfEvents = database.getForType(eventType).getNumberOfUniqueEvents()
                log.warn("Eventtypen '$eventType' ble kun telt for databasen, og ikke på topic. Fant $numberOfEvents eventer.")
            }
        }
    }

    fun eventTypesWithSessionFromBothSources(): List<EventType> {
        return eventTypesInBothSources
    }
}
