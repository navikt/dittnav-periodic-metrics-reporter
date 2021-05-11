package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext

class HealthService(private val applicationContext: ApplicationContext) {

    suspend fun getHealthChecks(): List<HealthStatus> {
        return listOf(
                applicationContext.databaseOnPrem.status(),
                applicationContext.periodicMetricsSubmitter.status(),
                applicationContext.periodicConsumerCheck.status(),
                applicationContext.beskjedCountConsumerOnPrem.status(),
                applicationContext.innboksCountConsumerOnPrem.status(),
                applicationContext.oppgaveCountConsumerOnPrem.status(),
                applicationContext.statusoppdateringCountConsumerOnPrem.status(),
                applicationContext.doneCountConsumerOnPrem.status(),
                applicationContext.beskjedCountConsumerAiven.status(),
                applicationContext.innboksCountConsumerAiven.status(),
                applicationContext.oppgaveCountConsumerAiven.status(),
                applicationContext.statusoppdateringCountConsumerAiven.status(),
                applicationContext.doneCountConsumerAiven.status()
        )
    }
}
