package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.MetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.PrometheusMetricsCollector
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.DB_EVENTS_CACHED

class DBMetricsProbe(private val metricsReporter: MetricsReporter,
                     private val nameScrubber: ProducerNameScrubber) {

    suspend fun runWithMetrics(eventType: EventType, block: suspend DBMetricsSession.() -> Unit) {
        val session = DBMetricsSession(eventType)
        block.invoke(session)
        if(session.getNumberOfCachedEvents() > 0) {
            handleCachedEvents(session)
        }
    }

    suspend fun handleCachedEvents(session: DBMetricsSession) {
        session.getUniqueProducers().forEach { producer ->
            val numberOfCachedEvents = session.getNumberOfCachedEventsForProducer(producer)
            val eventType = session.eventType
            val producerPublicAlias = nameScrubber.getPublicAlias(producer)
            metricsReporter.registerDataPoint(
                    DB_EVENTS_CACHED,
                    listOf("counter" to numberOfCachedEvents).toMap(),
                    listOf("eventType" to eventType.toString(),
                            "producer" to producerPublicAlias).toMap()
            )
            PrometheusMetricsCollector.registerEventsCached(numberOfCachedEvents, eventType, producerPublicAlias)
        }
    }
}
