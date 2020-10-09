package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.junit.jupiter.api.Test
import org.amshove.kluent.`should be greater than`

internal class DbCountingMetricsProbeTest {

    @Test
    internal fun `Should calculate processing time`() {
        val probe = DbCountingMetricsProbe()
        val minimumProcessingTimeInMs: Long = 500
        val minimumProcessingTimeInNs: Long = minimumProcessingTimeInMs * 1000000
        val session = runBlocking {
            probe.runWithMetrics(EventType.BESKJED) {
                delay(minimumProcessingTimeInMs)
            }
        }

        session.getProcessingTime() `should be greater than` minimumProcessingTimeInNs
    }

}
