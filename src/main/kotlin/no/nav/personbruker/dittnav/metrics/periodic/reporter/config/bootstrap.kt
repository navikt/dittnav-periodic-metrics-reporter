package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import io.ktor.application.Application
import io.ktor.application.ApplicationStarted
import io.ktor.application.ApplicationStopPreparing
import io.ktor.application.install
import io.ktor.features.DefaultHeaders
import io.ktor.routing.routing
import io.prometheus.client.hotspot.DefaultExports
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.healthApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.cacheCountingApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.eventCountingApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.kafkaCountingApi
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter.metricsSubmitterApi

fun Application.mainModule(appContext: ApplicationContext = ApplicationContext()) {
    DefaultExports.initialize()
    install(DefaultHeaders)
    routing {
        healthApi(appContext.healthService)
        kafkaCountingApi(appContext.kafkaEventCounterService, appContext.kafkaTopicEventCounterService)
        cacheCountingApi(appContext.cacheEventCounterService)
        eventCountingApi(appContext.kafkaEventCounterService, appContext.cacheEventCounterService)
        metricsSubmitterApi(appContext)
    }

    configureStartupHook(appContext)
    configureShutdownHook(appContext)
}

private fun Application.configureStartupHook(appContext: ApplicationContext) {
    environment.monitor.subscribe(ApplicationStarted) {
        appContext.periodicMetricsSubmitter.start()
    }
}

private fun Application.configureShutdownHook(appContext: ApplicationContext) {
    environment.monitor.subscribe(ApplicationStopPreparing) {
        runBlocking {
            appContext.periodicMetricsSubmitter.stop()
        }
        appContext.database.dataSource.close()
        appContext.closeAllConsumers()
    }
}
