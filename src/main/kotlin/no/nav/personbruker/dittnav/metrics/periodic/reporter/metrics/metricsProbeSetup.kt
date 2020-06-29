package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.InfluxMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx.SensuClient
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsProbe

fun buildTopicMetricsProbe(environment: Environment, database: Database): TopicMetricsProbe {
    val metricsReporter = resolveMetricsReporter(environment)
    val nameResolver = ProducerNameResolver(database)
    val nameScrubber = ProducerNameScrubber(nameResolver)
    return TopicMetricsProbe(metricsReporter, nameScrubber)
}

fun buildDbEventCountingMetricsProbe(environment: Environment, database: Database): DbCountingMetricsProbe {
    val metricsReporter = resolveMetricsReporter(environment)
    val nameResolver = ProducerNameResolver(database)
    val nameScrubber = ProducerNameScrubber(nameResolver)
    return DbCountingMetricsProbe(metricsReporter, nameScrubber)
}

private fun resolveMetricsReporter(environment: Environment): MetricsReporter {
    return if (environment.sensuHost == "" || environment.sensuHost == "stub") {
        StubMetricsReporter()
    } else {
        val sensuClient = SensuClient(environment.sensuHost, environment.sensuPort.toInt())
        InfluxMetricsReporter(sensuClient, environment)
    }
}
