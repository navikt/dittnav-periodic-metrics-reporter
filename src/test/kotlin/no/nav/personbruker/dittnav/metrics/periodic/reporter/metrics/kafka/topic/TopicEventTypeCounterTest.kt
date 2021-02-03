package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.slot
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Beskjed
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import org.amshove.kluent.`should be greater than`
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration

internal class TopicEventTypeCounterTest {

    @Test
    internal fun `Should calculate processing time `() {

        mockkObject(UniqueKafkaEventIdentifierTransformer)
        mockkObject(TopicEventTypeCounter)

        val polledEvents: ConsumerRecords<Nokkel, GenericRecord> = mockk()
        val consumer: Consumer<GenericRecord> = mockk()


        val deltaCountingEnabled = true
        val counter = TopicEventTypeCounter(consumer, EventType.BESKJED, deltaCountingEnabled)

        every { consumer.kafkaConsumer.poll(any<Duration>()) } returns polledEvents
        every { polledEvents.isEmpty } returns true

        val sessionSlot = slot<TopicMetricsSession>()

        val minimumProcessingTimeInMs: Long = 500

        every { TopicEventTypeCounter.countBatch(polledEvents, capture(sessionSlot)) } coAnswers {
            sessionSlot.captured.countEvent(UniqueKafkaEventIdentifier("1", "test", "123"))
            sessionSlot.captured.countEvent(UniqueKafkaEventIdentifier("2", "test", "123"))
            sessionSlot.captured.countEvent(UniqueKafkaEventIdentifier("3", "test", "123"))
            delay(minimumProcessingTimeInMs)
        }

        val minimumProcessingTimeInNs: Long = minimumProcessingTimeInMs * 1000000
        val session = runBlocking {
            counter.countEventsAsync().await()
        }

        session.getProcessingTime () `should be greater than` minimumProcessingTimeInNs
    }
}