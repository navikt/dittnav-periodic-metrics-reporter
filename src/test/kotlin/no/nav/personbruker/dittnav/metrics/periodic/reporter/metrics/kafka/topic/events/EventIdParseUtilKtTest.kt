package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events

import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.events.parse.EventIdParser
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should be instance of`
import org.junit.jupiter.api.Test

internal class EventIdParseUtilKtTest {
    @Test
    fun `Should be able to recognize and parse valid UUIDS`() {
        val eventIdText = "12345678-1234-abcd-abcd-123456abcdef"

        val eventId = EventIdParser.parseEventId(eventIdText)

        eventId `should be instance of` EventIdUuid::class
    }

    @Test
    fun `Should be able to recognize and parse valid UUIDS when prefixed with a single character`() {
        val eventIdText = "A12345678-1234-abcd-abcd-123456abcdef"

        val eventId = EventIdParser.parseEventId(eventIdText)

        eventId `should be instance of` EventIdPrefixedUuid::class

        (eventId as EventIdPrefixedUuid).prefix `should be equal to` 'A'
    }

    @Test
    fun `Should be able to recognize and parse valid ULIDs`() {
        val eventIdText = "1234ABCDEFGHJKMNPQRSTVWXYZ"

        val eventId = EventIdParser.parseEventId(eventIdText)

        eventId `should be instance of` EventIdUlid::class
    }

    @Test
    fun `Should keep string as-is if eventId does not match a recognized format`() {
        val eventIdText = "customEventId1"

        val eventId = EventIdParser.parseEventId(eventIdText)

        eventId `should be instance of` EventIdPlainText::class

        (eventId as EventIdPlainText).stringValue `should be equal to` eventIdText
    }

    @Test
    fun `EventId objects should behave as expected with respect to sets`() {
        val eventIdsWithDuplicates = listOf(
            "1245678-1234-abcd-abcd-123456abcdef",
            "1245678-1234-abcd-abcd-123456abcdef",
            "A1245678-1234-abcd-abcd-123456abcdef",
            "A1245678-1234-abcd-abcd-123456abcdef",
            "1234ABCDEFGHJKMNPQRSTVWXYZ",
            "1234ABCDEFGHJKMNPQRSTVWXYZ",
            "customEventId1",
            "customEventId1"
        )

        val parsedEventIdSet = HashSet<EventId>()

        eventIdsWithDuplicates.forEach { eventIdString ->
            parsedEventIdSet.add(EventIdParser.parseEventId(eventIdString))
        }

        parsedEventIdSet.size `should be equal to` 4
    }
}