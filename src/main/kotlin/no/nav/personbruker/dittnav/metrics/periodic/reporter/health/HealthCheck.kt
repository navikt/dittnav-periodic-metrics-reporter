package no.nav.personbruker.dittnav.metrics.periodic.reporter.health

interface HealthCheck {

    suspend fun status(): HealthStatus

}
