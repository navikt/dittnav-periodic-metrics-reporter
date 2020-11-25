package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

internal class PeriodicMetricsSubmitterTest {

    @Test
    fun `Should report metrics for both kafka topics and the database cache`() {
        val metricsSubmitterService = mockk<MetricsSubmitterService>(relaxed = true)

        val submitter = PeriodicMetricsSubmitter(metricsSubmitterService, 1)

        runBlocking {
            submitter.submitMetrics()
        }

        coVerify(exactly = 1) { metricsSubmitterService.submitMetrics() }

        confirmVerified(metricsSubmitterService)
    }

}
