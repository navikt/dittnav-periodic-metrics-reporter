package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics

import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.Routing
import io.ktor.routing.get
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.CacheEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.KafkaEventCounterService

fun Routing.eventCountingApi(kafkaEventCounterService: KafkaEventCounterService, cacheEventCounterService: CacheEventCounterService) {

    get("/internal/count/all") {
        val kafkaEvents = kafkaEventCounterService.countUniqueEvents()
        val cachedEvents = cacheEventCounterService.countAllEvents()
        val responseText = "$kafkaEvents\n$cachedEvents"
        call.respondText(text = responseText, contentType = ContentType.Text.Plain)
    }

    get("/internal/count/all-legacy") {
        val kafkaEvents = kafkaEventCounterService.countAllEvents()
        val cachedEvents = cacheEventCounterService.countAllEvents()
        val responseText = "$kafkaEvents\n$cachedEvents"
        call.respondText(text = responseText, contentType = ContentType.Text.Plain)
    }

}
