package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.builders.domain.Eventtype
import no.nav.brukernotifikasjon.schemas.internal.Feilrespons
import no.nav.brukernotifikasjon.schemas.internal.NokkelFeilrespons
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedInternObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.AvroBeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.objectmother.ConsumerRecordsObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.AvroDoneInternObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.AvroDoneObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.AvroInnboksInternObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.AvroInnboksObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.UniqueKafkaEventIdentifier
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.AvroOppgaveInternObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.AvroOppgaveObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering.AvroStatusoppdateringInternObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering.AvroStatusoppdateringObjectMother
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.shouldNotBeNull
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import java.time.Instant

internal class UniqueKafkaEventIdentifierTransformerTest {

    @Test
    fun `Should transform external Beskjed to internal`() {
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
    fun `Should transform external BeskjedIntern to internal`() {
        val nokkelIntern = NokkelIntern("sysBruker1", "1", "12345678901")
        val beskjedIntern = AvroBeskjedInternObjectMother.createBeskjedIntern()
        val original: ConsumerRecord<NokkelIntern, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkelIntern, beskjedIntern)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkelIntern.getEventId()
        transformed.systembruker `should be equal to` nokkelIntern.getSystembruker()
        transformed.fodselsnummer `should be equal to` nokkelIntern.getFodselsnummer()
    }

    @Test
    fun `Should transform external Innboks to internal`() {
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
    fun `Should transform external InnboksIntern to internal`() {
        val nokkelIntern = NokkelIntern("sysBruker2", "2", "12345678901")
        val innboksEvent = AvroInnboksInternObjectMother.createInnboksIntern()
        val original: ConsumerRecord<NokkelIntern, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkelIntern, innboksEvent)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkelIntern.getEventId()
        transformed.systembruker `should be equal to` nokkelIntern.getSystembruker()
        transformed.fodselsnummer `should be equal to` nokkelIntern.getFodselsnummer()
    }

    @Test
    fun `Should transform external Oppgave to internal`() {
        val nokkel = Nokkel("sysBruker3", "3")
        val oppgave = AvroOppgaveObjectMother.createOppgave("Dummytekst")
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, oppgave)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` oppgave.getFodselsnummer()
    }

    @Test
    fun `Should transform external OppgaveIntern to internal`() {
        val nokkelIntern = NokkelIntern("sysBruker3", "3", "12345678901")
        val oppgave = AvroOppgaveInternObjectMother.createOppgaveIntern()
        val original: ConsumerRecord<NokkelIntern, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkelIntern, oppgave)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkelIntern.getEventId()
        transformed.systembruker `should be equal to` nokkelIntern.getSystembruker()
        transformed.fodselsnummer `should be equal to` nokkelIntern.getFodselsnummer()
    }

    @Test
    fun `Should transform external Statusoppdatering to internal`() {
        val nokkel = Nokkel("sysBruker3", "1")
        val statusoppdateringEvent = AvroStatusoppdateringObjectMother.createStatusoppdateringWithStatusGlobal("SENDT")
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, statusoppdateringEvent)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` statusoppdateringEvent.getFodselsnummer()
    }

    @Test
    fun `Should transform external StatusoppdateringIntern to internal`() {
        val nokkelIntern = NokkelIntern("sysBruker3", "1", "12345678901")
        val statusoppdateringEvent = AvroStatusoppdateringInternObjectMother.createStatusoppdateringIntern()
        val original: ConsumerRecord<NokkelIntern, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkelIntern, statusoppdateringEvent)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkelIntern.getEventId()
        transformed.systembruker `should be equal to` nokkelIntern.getSystembruker()
        transformed.fodselsnummer `should be equal to` nokkelIntern.getFodselsnummer()
    }

    @Test
    fun `Should transform external Done-event to internal`() {
        val nokkel = Nokkel("sysBruker3", "3")
        val done = AvroDoneObjectMother.createDone("4", "12345678901")
        val original: ConsumerRecord<Nokkel, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkel, done)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkel.getEventId()
        transformed.systembruker `should be equal to` nokkel.getSystembruker()
        transformed.fodselsnummer `should be equal to` done.getFodselsnummer()
    }

    @Test
    fun `Should transform external DoneIntern-event to internal`() {
        val nokkelIntern = NokkelIntern("sysBruker3", "3", "12345678901")
        val done = AvroDoneInternObjectMother.createDoneIntern()
        val original: ConsumerRecord<NokkelIntern, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkelIntern, done)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkelIntern.getEventId()
        transformed.systembruker `should be equal to` nokkelIntern.getSystembruker()
        transformed.fodselsnummer `should be equal to` nokkelIntern.getFodselsnummer()
    }

    @Test
    fun `Should transform Feilrespons to internal`() {
        val nokkelFeilrespons = NokkelFeilrespons("sysBruker4", "4", EventType.BESKJED.toString())
        val feilrespons = Feilrespons(Instant.now().toEpochMilli(), "Simulert feil i en test")
        val original: ConsumerRecord<NokkelFeilrespons, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecord(nokkelFeilrespons, feilrespons)

        val transformed = UniqueKafkaEventIdentifierTransformer.toInternal(original)

        transformed.eventId `should be equal to` nokkelFeilrespons.getEventId()
        transformed.systembruker `should be equal to` nokkelFeilrespons.getSystembruker()
        transformed.fodselsnummer `should be equal to` "000"
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
    fun `Should handle null as key (NokkelIntern)`() {
        val doneIntern = AvroDoneInternObjectMother.createDoneIntern()
        val recordWithouKey: ConsumerRecord<NokkelIntern, GenericRecord> =
            ConsumerRecordsObjectMother.createConsumerRecordWithoutNokkel(doneIntern)
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
