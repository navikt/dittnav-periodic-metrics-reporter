package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import io.ktor.application.*
import io.ktor.features.*
import io.ktor.routing.*
import io.prometheus.client.hotspot.DefaultExports
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.healthApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter.metricsSubmitterApi

fun Application.mainModule(appContext: ApplicationContext = ApplicationContext()) {
    DefaultExports.initialize()
    install(DefaultHeaders)
    routing {
        healthApi(appContext.healthService)
        metricsSubmitterApi(appContext)
    }

    configureStartupHook(appContext)
    configureShutdownHook(appContext)
}

private fun Application.configureStartupHook(appContext: ApplicationContext) {
    environment.monitor.subscribe(ApplicationStarted) {
        KafkaConsumerSetup.startSubscriptionOnAllKafkaConsumers(appContext)
        appContext.periodicMetricsSubmitter.start()
        appContext.periodicConsumerPollingCheck.start()
    }
}

private fun Application.configureShutdownHook(appContext: ApplicationContext) {
    environment.monitor.subscribe(ApplicationStopPreparing) {
        runBlocking {
            appContext.periodicMetricsSubmitter.stop()
            appContext.periodicConsumerPollingCheck.stop()
            KafkaConsumerSetup.stopAllKafkaConsumers(appContext)
        }
        appContext.database.dataSource.close()
    }
}
