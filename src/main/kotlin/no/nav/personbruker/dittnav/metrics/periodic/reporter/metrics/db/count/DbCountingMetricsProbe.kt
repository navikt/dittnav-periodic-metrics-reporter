package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.PrometheusMetricsCollector
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.DB_COUNT_PROCESSING_TIME
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.DB_TOTAL_EVENTS_IN_CACHE
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER
import org.slf4j.LoggerFactory

class DbCountingMetricsProbe(
    private val metricsReporter: MetricsReporter,
    private val nameScrubber: ProducerNameScrubber
) {

    private val log = LoggerFactory.getLogger(DbCountingMetricsProbe::class.java)

    suspend fun runWithMetrics(eventType: EventType, block: suspend DbCountingMetricsSession.() -> Unit) {
        val start = System.nanoTime()
        val session = DbCountingMetricsSession(eventType)
        block.invoke(session)
        val processingTime = System.nanoTime() - start

        if (session.getProducers().isNotEmpty()) {
            handleTotalEvents(session)
            handleTotalEventsByProducer(session)
            reportTimeUsed(session, processingTime)
        }
    }

    private suspend fun handleTotalEvents(session: DbCountingMetricsSession) {
        val numberOfEvents = session.getTotalNumber()
        val eventTypeName = session.eventType.toString()

        reportEvents(numberOfEvents, eventTypeName, DB_TOTAL_EVENTS_IN_CACHE)
        PrometheusMetricsCollector.registerTotalNumberOfEventsInCache(numberOfEvents, session.eventType)
    }

    private suspend fun handleTotalEventsByProducer(session: DbCountingMetricsSession) {
        session.getProducers().forEach { producerName ->
            val numberOfEvents = session.getNumberOfEventsFor(producerName)
            val eventTypeName = session.eventType.toString()
            val printableAlias = nameScrubber.getPublicAlias(producerName)

            reportEvents(numberOfEvents, eventTypeName, printableAlias, DB_TOTAL_EVENTS_IN_CACHE_BY_PRODUCER)
            PrometheusMetricsCollector.registerTotalNumberOfEventsInCacheByProducer(
                numberOfEvents,
                session.eventType,
                printableAlias
            )
        }
    }

    private suspend fun reportTimeUsed(session: DbCountingMetricsSession, processingTime: Long) {
        reportProcessingTimeEvent(processingTime, session)
        PrometheusMetricsCollector.registerProcessingTimeInCache(processingTime, session.eventType)
    }

    private suspend fun reportEvents(count: Int, eventType: String, producerAlias: String, metricName: String) {
        metricsReporter.registerDataPoint(metricName, counterField(count), createTagMap(eventType, producerAlias))
    }

    private fun counterField(events: Int): Map<String, Int> = listOf("counter" to events).toMap()

    private fun counterField(events: Long): Map<String, Long> = listOf("counter" to events).toMap()

    private fun createTagMap(eventType: String, producer: String): Map<String, String> =
        listOf("eventType" to eventType, "producer" to producer).toMap()

    private suspend fun reportEvents(count: Int, eventType: String, metricName: String) {
        metricsReporter.registerDataPoint(metricName, counterField(count), createTagMap(eventType))
    }

    private suspend fun reportProcessingTimeEvent(processingTime: Long, session: DbCountingMetricsSession) {
        val metricName = DB_COUNT_PROCESSING_TIME
        metricsReporter.registerDataPoint(
            metricName, counterField(processingTime),
            createTagMap(session.eventType.eventType)
        )
    }

    private fun createTagMap(eventType: String): Map<String, String> =
        listOf("eventType" to eventType).toMap()

}
