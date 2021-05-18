package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.personbruker.dittnav.common.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.MetricsReportingException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.*
import org.slf4j.LoggerFactory

class TopicMetricsReporter(
        private val metricsReporter: MetricsReporter,
        private val nameScrubber: ProducerNameScrubber
) {

    private val log = LoggerFactory.getLogger(TopicMetricsReporter::class.java)

    suspend fun report(session: TopicMetricsSession) {
        try {
            reportIfEventsCounted(session)

        } catch (e: Exception) {
            val msg = "Klarte ikke å rapportere topic-metrikker for ${session.eventType.eventType}"
            throw MetricsReportingException(msg, e)
        }
    }

    private suspend fun reportIfEventsCounted(session: TopicMetricsSession) {
        if (session.getNumberOfUniqueEvents() > 0) {
            reportUniqueEvents(session)
            reportTotalNumberOfEvents(session)
            reportUniqueEventsByProducer(session)
            reportDuplicatedEventsByProducer(session)
            reportTotalNumberOfEventsByProducer(session)
            reportTimeUsed(session)

        } else {
            log.info("Ingen eventer ble funnet på topic for ${session.eventType}.")
        }
    }

    private suspend fun reportUniqueEvents(session: TopicMetricsSession) {
        val uniqueEvents = session.getNumberOfUniqueEvents()
        val eventTypeName = session.eventType.toString()

        reportEvents(uniqueEvents, eventTypeName, KAFKA_UNIQUE_EVENTS_ON_TOPIC)
        PrometheusMetricsCollector.registerUniqueEvents(uniqueEvents, session.eventType)
    }

    private suspend fun reportTotalNumberOfEvents(session: TopicMetricsSession) {
        val total = session.getTotalNumber()
        val eventTypeName = session.eventType.toString()

        reportEvents(total, eventTypeName, KAFKA_TOTAL_EVENTS_ON_TOPIC)
        PrometheusMetricsCollector.registerTotalNumberOfEvents(total, session.eventType)
    }

    private suspend fun reportUniqueEventsByProducer(session: TopicMetricsSession) {
        session.getProducersWithUniqueEvents().forEach { producerName ->
            val uniqueEvents = session.getNumberOfUniqueEvents(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(uniqueEvents, eventTypeName, printableAlias, KAFKA_UNIQUE_EVENTS_ON_TOPIC_BY_PRODUCER)
            PrometheusMetricsCollector.registerUniqueEventsByProducer(uniqueEvents, session.eventType, printableAlias)
        }
    }

    private suspend fun reportDuplicatedEventsByProducer(session: TopicMetricsSession) {
        session.getProducersWithDuplicatedEvents().forEach { producerName ->
            val duplicates = session.getDuplicates(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(duplicates, eventTypeName, printableAlias, KAFKA_DUPLICATE_EVENTS_ON_TOPIC)
            PrometheusMetricsCollector.registerDuplicatedEventsOnTopic(duplicates, session.eventType, printableAlias)
        }
    }

    private suspend fun reportTotalNumberOfEventsByProducer(session: TopicMetricsSession) {
        session.getProducersWithEvents().forEach { producerName ->
            val total = session.getTotalNumber(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(total, eventTypeName, printableAlias, KAFKA_TOTAL_EVENTS_ON_TOPIC_BY_PRODUCER)
            PrometheusMetricsCollector.registerTotalNumberOfEventsByProducer(total, session.eventType, printableAlias)
        }
    }

    private suspend fun reportTimeUsed(session: TopicMetricsSession) {
        reportProcessingTimeEvent(session)
        PrometheusMetricsCollector.registerProcessingTime(session.getProcessingTime(), session.eventType)
    }

    private suspend fun reportEvents(count: Int, eventType: String, producerAlias: String, metricName: String) {
        metricsReporter.registerDataPoint(metricName, counterField(count), createTagMap(eventType, producerAlias))
    }

    private suspend fun reportProcessingTimeEvent(session: TopicMetricsSession) {
        val metricName = KAFKA_COUNT_PROCESSING_TIME
        metricsReporter.registerDataPoint(
            metricName, counterField(session.getProcessingTime()),
            createTagMap(session.eventType.eventType)
        )
    }

    private fun counterField(events: Int): Map<String, Int> = listOf("counter" to events).toMap()

    private fun counterField(events: Long): Map<String, Long> = listOf("counter" to events).toMap()

    private fun createTagMap(eventType: String, producer: String): Map<String, String> =
        listOf("eventType" to eventType, "producer" to producer).toMap()

    private suspend fun reportEvents(count: Int, eventType: String, metricName: String) {
        metricsReporter.registerDataPoint(metricName, counterField(count), createTagMap(eventType))
    }

    private fun createTagMap(eventType: String): Map<String, String> =
        listOf("eventType" to eventType).toMap()

}
