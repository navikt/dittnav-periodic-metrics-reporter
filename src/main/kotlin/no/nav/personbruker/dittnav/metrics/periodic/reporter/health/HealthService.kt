package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext

class HealthService(private val applicationContext: ApplicationContext) {

    suspend fun getHealthChecks(): List<HealthStatus> {
        return listOf(
                applicationContext.database.status(),
                applicationContext.beskjedConsumer.status(),
                applicationContext.innboksConsumer.status(),
                applicationContext.oppgaveConsumer.status(),
                applicationContext.doneConsumer.status(),
                applicationContext.periodicDoneEventWaitingTableProcessor.status(),
                applicationContext.periodicMetricsSubmitter.status()
        )
    }
}
