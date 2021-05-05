package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext

class HealthService(private val applicationContext: ApplicationContext) {

    suspend fun getHealthChecks(): List<HealthStatus> {
        return listOf(
                applicationContext.databaseOnPrem.status(),
                applicationContext.databaseGCP.status(),
                applicationContext.periodicMetricsSubmitter.status(),
                applicationContext.periodicConsumerCheck.status(),
                applicationContext.beskjedCountConsumerOnPrem.status(),
                applicationContext.innboksCountConsumerOnPrem.status(),
                applicationContext.oppgaveCountConsumerOnPrem.status(),
                applicationContext.statusoppdateringCountConsumerOnPrem.status(),
                applicationContext.doneCountConsumerOnPrem.status(),
                applicationContext.beskjedCountConsumerGCP.status(),
                applicationContext.innboksCountConsumerGCP.status(),
                applicationContext.oppgaveCountConsumerGCP.status(),
                applicationContext.statusoppdateringCountConsumerGCP.status(),
                applicationContext.doneCountConsumerGCP.status()
        )
    }
}
