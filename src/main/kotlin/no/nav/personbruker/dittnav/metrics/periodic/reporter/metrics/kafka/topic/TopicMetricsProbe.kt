package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.PrometheusMetricsCollector
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.*
import org.slf4j.LoggerFactory

class TopicMetricsProbe(private val metricsReporter: MetricsReporter,
                        private val nameScrubber: ProducerNameScrubber) {

    private val log = LoggerFactory.getLogger(TopicMetricsProbe::class.java)

    private val lastReportedUniqueEvents = HashMap<EventType, Int>()

    suspend fun runWithMetrics(eventType: EventType, block: suspend TopicMetricsSession.() -> Unit) {
        val session = TopicMetricsSession(eventType)
        block.invoke(session)

        if (countedMoreEventsThanLastCount(session, eventType)) {
            handleUniqueEvents(session)
            handleTotalNumberOfEvents(session)
            handleUniqueEventsByProducer(session)
            handleDuplicatedEventsByProducer(session)
            handleTotalNumberOfEventsByProducer(session)
            lastReportedUniqueEvents[eventType] = session.getNumberOfUniqueEvents()

        } else {
            val currentCount = session.getNumberOfUniqueEvents()
            val previousCount = lastReportedUniqueEvents.getOrDefault(eventType, 0)
            val msg = "Det har oppstått en tellefeil, rapporterer derfor ikke nye metrikker. " +
                    "Antall unike eventer ved forrige rapportering $previousCount, antall telt nå $currentCount."
            log.warn(msg)
        }
    }

    private fun countedMoreEventsThanLastCount(session: TopicMetricsSession, eventType: EventType) =
            session.getNumberOfUniqueEvents() >= lastReportedUniqueEvents.getOrDefault(eventType, 0)

    private suspend fun handleUniqueEvents(session: TopicMetricsSession) {
        val uniqueEvents = session.getNumberOfUniqueEvents()
        val eventTypeName = session.eventType.toString()

        reportEvents(uniqueEvents, eventTypeName, KAFKA_UNIQUE_EVENTS_ON_TOPIC)
        PrometheusMetricsCollector.registerUniqueEvents(uniqueEvents, session.eventType)
    }

    private suspend fun handleTotalNumberOfEvents(session: TopicMetricsSession) {
        val total = session.getTotalNumber()
        val eventTypeName = session.eventType.toString()

        reportEvents(total, eventTypeName, KAFKA_TOTAL_EVENTS_ON_TOPIC)
        PrometheusMetricsCollector.registerTotalNumberOfEvents(total, session.eventType)
    }

    private suspend fun handleUniqueEventsByProducer(session: TopicMetricsSession) {
        session.getProducersWithUniqueEvents().forEach { producerName ->
            val uniqueEvents = session.getNumberOfUniqueEvents(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(uniqueEvents, eventTypeName, printableAlias, KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER)
            PrometheusMetricsCollector.registerUniqueEventsByProducer(uniqueEvents, session.eventType, printableAlias)
        }
    }

    private suspend fun handleDuplicatedEventsByProducer(session: TopicMetricsSession) {
        session.getProducersWithDuplicatedEvents().forEach { producerName ->
            val duplicates = session.getDuplicates(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(duplicates, eventTypeName, printableAlias, KAFKA_DUPLICATE_EVENTS_ON_TOPIC)
            PrometheusMetricsCollector.registerDuplicatedEventsOnTopic(duplicates, session.eventType, printableAlias)
        }
    }

    private suspend fun handleTotalNumberOfEventsByProducer(session: TopicMetricsSession) {
        session.getProducersWithEvents().forEach { producerName ->
            val total = session.getTotalNumber(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(total, eventTypeName, printableAlias, KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER)
            PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(total, session.eventType, printableAlias)
        }
    }

    private suspend fun reportEvents(count: Int, eventType: String, producerAlias: String, metricName: String) {
        metricsReporter.registerDataPoint(metricName, counterField(count), createTagMap(eventType, producerAlias))
    }

    private fun counterField(events: Int): Map<String, Int> = listOf("counter" to events).toMap()

    private fun createTagMap(eventType: String, producer: String): Map<String, String> =
            listOf("eventType" to eventType, "producer" to producer).toMap()

    private suspend fun reportEvents(count: Int, eventType: String, metricName: String) {
        metricsReporter.registerDataPoint(metricName, counterField(count), createTagMap(eventType))
    }

    private fun createTagMap(eventType: String): Map<String, String> =
            listOf("eventType" to eventType).toMap()

}
