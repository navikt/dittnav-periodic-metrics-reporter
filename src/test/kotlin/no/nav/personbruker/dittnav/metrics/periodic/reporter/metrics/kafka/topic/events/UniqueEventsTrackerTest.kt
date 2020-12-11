package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Test

internal class UniqueEventsTrackerTest {
    @Test
    fun `Should handle no events registered`() {
        val tracker = UniqueEventsTracker()

        tracker.uniqueEvents `should be equal to` 0
    }

    @Test
    fun `Should keep track of unique events`() {
        val eventWithDuplicates1 = (1..3).map { createEvent("123", "srv_one","ab-1") }
        val eventWithDuplicates2 = (1..2).map { createEvent("123", "srv_one","ab-2") }
        val eventWithDuplicates3 = (1..3).map { createEvent("456", "srv_one","ab-1") }
        val producerOneEvents = eventWithDuplicates1 + eventWithDuplicates2 + eventWithDuplicates3

        val eventWithDuplicates4 = (1..4).map { createEvent("123", "srv_two","ab-1") }
        val eventWithDuplicates5 = (1..1).map { createEvent("123", "srv_two","ab-2") }
        val eventWithDuplicates6 = (1..5).map { createEvent("456", "srv_two","ab-1") }
        val producerTwoEvents = eventWithDuplicates4 + eventWithDuplicates5 + eventWithDuplicates6

        val allEvents = producerOneEvents + producerTwoEvents

        val tracker = UniqueEventsTracker()

        allEvents.forEach { tracker.addEvent(it) }

        tracker.uniqueEvents `should be equal to` 6
    }
}

private fun createEvent(ident: String, systemBruker: String, eventId: String)
        = UniqueKafkaEventIdentifier(eventId, systemBruker, ident)