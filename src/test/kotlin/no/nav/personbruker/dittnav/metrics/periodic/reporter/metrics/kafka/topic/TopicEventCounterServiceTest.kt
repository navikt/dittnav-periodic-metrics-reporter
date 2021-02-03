package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.`with message containing`
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.amshove.kluent.`should throw`
import org.amshove.kluent.invoking
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test
import java.time.Duration

internal class TopicEventCounterServiceTest {

    private val beskjedCountConsumer: Consumer<GenericRecord> = mockk(relaxed = true)
    private val innboksCountConsumer: Consumer<GenericRecord> = mockk(relaxed = true)
    private val oppgaveCountConsumer: Consumer<GenericRecord> = mockk(relaxed = true)
    private val doneCountConsumer: Consumer<GenericRecord> = mockk(relaxed = true)
//    private val doneCountConsumer: KafkaConsumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val beskjedCounter = TopicEventTypeCounter(beskjedCountConsumer, EventType.BESKJED, false)
    private val innboksCounter = TopicEventTypeCounter(innboksCountConsumer, EventType.INNBOKS, false)
    private val oppgaveCounter = TopicEventTypeCounter(oppgaveCountConsumer, EventType.OPPGAVE, false)
    private val doneCounter = TopicEventTypeCounter(doneCountConsumer, EventType.DONE, false)

    @Test
    internal fun `Should handle exceptions and rethrow as internal exception`() {
        val simulatedException = Exception("Simulated error in a test")
        coEvery { beskjedCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { innboksCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { oppgaveCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { doneCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException

        invoking {
            runBlocking {
                beskjedCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "beskjed"

        invoking {
            runBlocking {
                doneCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "done"

        invoking {
            runBlocking {
                innboksCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "innboks"

        invoking {
            runBlocking {
                oppgaveCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "oppgave"

    }
}
