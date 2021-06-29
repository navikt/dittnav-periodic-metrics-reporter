package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.internal.NokkelFeilrespons
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.`with message containing`
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
import org.amshove.kluent.`should throw`
import org.amshove.kluent.invoking
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Test
import java.time.Duration

internal class TopicEventCounterServiceTest {

    private val beskjedCountConsumer: Consumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val beskjedInternCountConsumer: Consumer<NokkelIntern, GenericRecord> = mockk(relaxed = true)
    private val innboksCountConsumer: Consumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val innboksInternCountConsumer: Consumer<NokkelIntern, GenericRecord> = mockk(relaxed = true)
    private val oppgaveCountConsumer: Consumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val oppgaveInternCountConsumer: Consumer<NokkelIntern, GenericRecord> = mockk(relaxed = true)
    private val statusoppdateringCountConsumer: Consumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val statusoppdateringInternCountConsumer: Consumer<NokkelIntern, GenericRecord> = mockk(relaxed = true)
    private val doneCountConsumer: Consumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val doneInternCountConsumer: Consumer<NokkelIntern, GenericRecord> = mockk(relaxed = true)
    private val feilresponsCountConsumer: Consumer<NokkelFeilrespons, GenericRecord> = mockk(relaxed = true)
    private val beskjedActivityService: TopicActivityService = mockk()
    private val beskjedInternActivityService: TopicActivityService = mockk()
    private val innboksActivityService: TopicActivityService = mockk()
    private val innboksInternActivityService: TopicActivityService = mockk()
    private val oppgaveActivityService: TopicActivityService = mockk()
    private val oppgaveInternActivityService: TopicActivityService = mockk()
    private val statusoppdateringActivityService: TopicActivityService = mockk()
    private val statusoppdateringInternActivityService: TopicActivityService = mockk()
    private val doneActivityService: TopicActivityService = mockk()
    private val doneInternActivityService: TopicActivityService = mockk()
    private val feilresponsActivityService: TopicActivityService = mockk()
    private val beskjedCounter = TopicEventTypeCounter(beskjedCountConsumer, beskjedActivityService, EventType.BESKJED, false)
    private val beskjedInternCounter = TopicEventTypeCounter(beskjedInternCountConsumer, beskjedInternActivityService, EventType.BESKJED_INTERN, false)
    private val innboksCounter = TopicEventTypeCounter(innboksCountConsumer, innboksActivityService, EventType.INNBOKS, false)
    private val innboksInternCounter = TopicEventTypeCounter(innboksInternCountConsumer, innboksInternActivityService, EventType.INNBOKS_INTERN, false)
    private val oppgaveCounter = TopicEventTypeCounter(oppgaveCountConsumer, oppgaveActivityService, EventType.OPPGAVE, false)
    private val oppgaveInternCounter = TopicEventTypeCounter(oppgaveInternCountConsumer, oppgaveInternActivityService, EventType.OPPGAVE_INTERN, false)
    private val statusoppdateringCounter = TopicEventTypeCounter(statusoppdateringCountConsumer, statusoppdateringActivityService, EventType.STATUSOPPDATERING, false)
    private val statusoppdateringInternCounter = TopicEventTypeCounter(statusoppdateringInternCountConsumer, statusoppdateringInternActivityService, EventType.STATUSOPPDATERING_INTERN, false)
    private val doneCounter = TopicEventTypeCounter(doneCountConsumer, doneActivityService, EventType.DONE, false)
    private val doneInternCounter = TopicEventTypeCounter(doneInternCountConsumer, doneInternActivityService, EventType.DONE_INTERN, false)
    private val feilresponsCounter = TopicEventTypeCounter(feilresponsCountConsumer, feilresponsActivityService, EventType.FEILRESPONS, false)

    @Test
    internal fun `Should handle exceptions and rethrow as internal exception`() {
        val simulatedException = Exception("Simulated error in a test")
        coEvery { beskjedCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { beskjedInternCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { innboksCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { innboksInternCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { oppgaveCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { oppgaveInternCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { statusoppdateringCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { statusoppdateringInternCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { doneCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { doneInternCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException
        coEvery { feilresponsCountConsumer.kafkaConsumer.poll(any<Duration>()) } throws simulatedException

        invoking {
            runBlocking {
                beskjedCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "beskjed"

        invoking {
            runBlocking {
                beskjedInternCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "beskjed_intern"

        invoking {
            runBlocking {
                doneCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "done"

        invoking {
            runBlocking {
                doneInternCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "done_intern"

        invoking {
            runBlocking {
                innboksCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "innboks"

        invoking {
            runBlocking {
                innboksInternCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "innboks_intern"

        invoking {
            runBlocking {
                oppgaveCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "oppgave"

        invoking {
            runBlocking {
                oppgaveInternCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "oppgave_intern"

        invoking {
            runBlocking {
                statusoppdateringCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "statusoppdatering"

        invoking {
            runBlocking {
                statusoppdateringInternCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "statusoppdatering_intern"

        invoking {
            runBlocking {
                feilresponsCounter.countEventsAsync()
            }
        } `should throw` CountException::class `with message containing` "feilrespons"
    }
}
