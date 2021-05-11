package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling

import io.ktor.application.*
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.Routing
import io.ktor.routing.get
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup

fun Routing.consumerApi(appContext: ApplicationContext) {

    get("/internal/consumer/start") {
        val responseText = "Konsumerne har blitt restartet, både on-prem og Aiven."
        KafkaConsumerSetup.restartConsumersOnPrem(appContext)
        KafkaConsumerSetup.restartConsumersAiven(appContext)
        call.respondText(text = responseText, contentType = ContentType.Text.Plain)
    }

    get("/internal/consumer/stop") {
        val responseText = "Stoppet alle konsumere, både on-prem og Aiven."
        KafkaConsumerSetup.stopAllKafkaConsumersOnPrem(appContext)
        KafkaConsumerSetup.stopAllKafkaConsumersAiven(appContext)
        call.respondText(text = responseText, contentType = ContentType.Text.Plain)
    }

    get("/internal/consumer/checker/start") {
        val responseText = "Startet jobben som sjekker om konsumerne kjører."
        appContext.reinitializePeriodicConsumerCheck()
        appContext.periodicConsumerCheck.start()
        call.respondText(text = responseText, contentType = ContentType.Text.Plain)
    }

    get("/internal/consumer/checker/stop") {
        val responseText = "Stoppet jobben som sjekker om konsumerne kjører."
        appContext.periodicConsumerCheck.stop()
        call.respondText(text = responseText, contentType = ContentType.Text.Plain)
    }

}
