package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.`with message containing`
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import org.amshove.kluent.`should throw`
import org.amshove.kluent.invoking
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.Test

internal class TopicEventCounterServiceTest {

    private val beskjedCountConsumer: KafkaConsumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val innboksCountConsumer: KafkaConsumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val oppgaveCountConsumer: KafkaConsumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val doneCountConsumer: KafkaConsumer<Nokkel, GenericRecord> = mockk(relaxed = true)
    private val topicMetricsProbe: TopicMetricsProbe = mockk(relaxed = true)

    private val topicEventCounterService = TopicEventCounterService(
        topicMetricsProbe,
        beskjedCountConsumer,
        innboksCountConsumer,
        oppgaveCountConsumer,
        doneCountConsumer
    )

    @Test
    internal fun `Should handle exceptions and rethrow as internal exception`() {
        val simulatedException = Exception("Simulated error in a test")
        coEvery { topicMetricsProbe.runWithMetrics(any(), any()) } throws simulatedException

        invoking {
            runBlocking {
                topicEventCounterService.countBeskjeder()
            }
        } `should throw` CountException::class `with message containing` "beskjed"

        invoking {
            runBlocking {
                topicEventCounterService.countDoneEvents()
            }
        } `should throw` CountException::class `with message containing` "done"

        invoking {
            runBlocking {
                topicEventCounterService.countInnboksEventer()
            }
        } `should throw` CountException::class `with message containing` "innboks"

        invoking {
            runBlocking {
                topicEventCounterService.countOppgaver()
            }
        } `should throw` CountException::class `with message containing` "oppgave"

    }
}
