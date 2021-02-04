package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext

class HealthService(private val applicationContext: ApplicationContext) {

    suspend fun getHealthChecks(): List<HealthStatus> {
        return listOf(
                applicationContext.database.status(),
                applicationContext.periodicMetricsSubmitter.status(),
                applicationContext.periodicConsumerPollingCheck.status(),
                applicationContext.beskjedCountConsumer.status(),
                applicationContext.innboksCountConsumer.status(),
                applicationContext.oppgaveCountConsumer.status(),
                applicationContext.doneCountConsumer.status()
        )
    }
}
