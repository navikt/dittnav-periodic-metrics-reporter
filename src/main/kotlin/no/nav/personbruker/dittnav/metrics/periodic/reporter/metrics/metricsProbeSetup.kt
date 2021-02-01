package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics

import no.nav.personbruker.dittnav.common.metrics.MetricsReporter
import no.nav.personbruker.dittnav.common.metrics.StubMetricsReporter
import no.nav.personbruker.dittnav.common.metrics.influx.InfluxMetricsReporter
import no.nav.personbruker.dittnav.common.metrics.influx.SensuConfig
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment

fun resolveMetricsReporter(environment: Environment): MetricsReporter {
    return if (environment.sensuHost == "" || environment.sensuHost == "stub") {
        StubMetricsReporter()
    } else {
        val sensuConfig = createSensuConfig(environment)
        InfluxMetricsReporter(sensuConfig)
    }
}

fun createSensuConfig(environment: Environment) = SensuConfig(
        hostName = environment.sensuHost,
        hostPort = environment.sensuPort,
        clusterName = environment.clusterName,
        namespace = environment.namespace,
        applicationName = "dittnav-periodic-metrics-reporter",
        eventsTopLevelName = "aggregator-kafka-events"
)
