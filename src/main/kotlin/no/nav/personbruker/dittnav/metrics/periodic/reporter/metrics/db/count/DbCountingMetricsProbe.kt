package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.slf4j.LoggerFactory

class DbCountingMetricsProbe {

    private val log = LoggerFactory.getLogger(DbCountingMetricsProbe::class.java)

    suspend fun runWithMetrics(eventType: EventType, block: suspend DbCountingMetricsSession.() -> Unit): DbCountingMetricsSession {
        val session = DbCountingMetricsSession(eventType)
        block.invoke(session)
        session.calculateProcessingTime()
        return session
    }

}
