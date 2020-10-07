package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.objectmother.ConsumerRecordsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.schema.AvroDoneObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.AvroInnboksObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.AvroOppgaveObjectMother
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.shouldNotBeNull
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test

internal class UniqueKafkaEventIdentifierTransformerTest {

    @Test
    fun `Should transform external beskjed to internal`() {
        val nokkel = Nokkel("sysBruker1", "1")
        val beskjed = AvroBeskjedObjectMother.createBeskjedWithoutSynligFremTilSatt()
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, beskjed)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` beskjed.getFodselsnummer()
    }

    @Test
    fun `Should transform external innboks-event to internal`() {
        val nokkel = Nokkel("sysBruker2", "2")
        val innboksEvent = AvroInnboksObjectMother.createInnboksWithText("Dummytekst")
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, innboksEvent)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` innboksEvent.getFodselsnummer()
    }

    @Test
    fun `Should transform external oppgave to internal`() {
        val nokkel = Nokkel("sysBruker3", "3")
        val innboksEvent = AvroOppgaveObjectMother.createOppgave("Dummytekst")
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, innboksEvent)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` innboksEvent.getFodselsnummer()
    }

    @Test
    fun `Should transform external done-event to internal`() {
        val nokkel = Nokkel("sysBruker3", "3")
        val done = AvroDoneObjectMother.createDone("4", "12345")
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, done)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` done.getFodselsnummer()
    }

    @Test
    fun `Should handle null as key (Nokkel)`() {
        val done = AvroDoneObjectMother.createDone("5", "123456")
        val recordWithouKey: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecordWithoutNokkel(done)
        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(recordWithouKey)

        transformed.shouldNotBeNull()
        transformed `should be equal to` UniqueKafkaEventIdentifier.createInvalidEvent()
    }

    @Test
    fun `Should handle null as value (record)`() {
        val nokkel = Nokkel("sysBruker3", "6")
        val recordWithoutValue: ConsumerRecord<Nokkel, GenericRecord> = ConsumerRecordsObjectMother.createConsumerRecordWithoutRecord(nokkel)
        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(recordWithoutValue)

        transformed.shouldNotBeNull()
        transformed `should be equal to` UniqueKafkaEventIdentifier.createEventWithoutValidFnr(
            nokkel.getEventId(),
            nokkel.getSystembruker()
        )
    }

}
