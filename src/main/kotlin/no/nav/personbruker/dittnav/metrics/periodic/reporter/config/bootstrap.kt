package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import io.ktor.application.*
import io.ktor.features.*
import io.ktor.routing.*
import io.prometheus.client.hotspot.DefaultExports
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling.consumerApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.healthApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter.metricsSubmitterApi
import org.slf4j.LoggerFactory

val log = LoggerFactory.getLogger(ApplicationContext::class.java)

fun Application.mainModule(appContext: ApplicationContext = ApplicationContext()) {
    DefaultExports.initialize()
    install(DefaultHeaders)
    routing {
        healthApi(appContext.healthService, appContext.activityHealthService)
        metricsSubmitterApi(appContext)
        consumerApi(appContext)
    }

    configureStartupHook(appContext)
    configureShutdownHook(appContext)

    log.info("Delta counting enabled: ${appContext.environment.deltaCountingEnabled}")
}

private fun Application.configureStartupHook(appContext: ApplicationContext) {
    environment.monitor.subscribe(ApplicationStarted) {
        KafkaConsumerSetup.startSubscriptionOnAllKafkaConsumersOnPrem(appContext)
        KafkaConsumerSetup.startSubscriptionOnAllKafkaConsumersAiven(appContext)
        appContext.periodicConsumerCheck.start()
        appContext.periodicMetricsSubmitter.start()
    }
}

private fun Application.configureShutdownHook(appContext: ApplicationContext) {
    environment.monitor.subscribe(ApplicationStopPreparing) {
        runBlocking {
            appContext.periodicConsumerCheck.stop()
            appContext.periodicMetricsSubmitter.stop()
            KafkaConsumerSetup.stopAllKafkaConsumersOnPrem(appContext)
            KafkaConsumerSetup.stopAllKafkaConsumersAiven(appContext)
        }
        appContext.databaseOnPrem.dataSource.close()
    }
}
