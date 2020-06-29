package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.influx

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.MetricsReporter
import org.influxdb.dto.Point
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class InfluxMetricsReporter(val sensuClient: SensuClient, val environment: Environment) : MetricsReporter {

    val log = LoggerFactory.getLogger(InfluxMetricsReporter::class.java)

    override suspend fun registerDataPoint(measurement: String, fields: Map<String, Any>, tags: Map<String, String>) {
        val point = Point.measurement(measurement)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag(tags)
                .tag(DEFAULT_TAGS)
                .fields(fields)
                .build()

        sensuClient.submitEvent(SensuEvent(point))
    }

    private val DEFAULT_TAGS = listOf(
        "application" to "dittnav-periodic-metrics-reporter",
        "cluster" to environment.clusterName,
        "namespace" to environment.namespace
    ).toMap()
}