package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.slf4j.LoggerFactory

class TopicMetricsProbe {

    private val log = LoggerFactory.getLogger(TopicMetricsProbe::class.java)

    suspend fun runWithMetrics(eventType: EventType, block: suspend TopicMetricsSession.() -> Unit): TopicMetricsSession {
        val session = TopicMetricsSession(eventType)
        block.invoke(session)
        session.calculateProcessingTime()
        return session
    }

}
