package no.nav.personbruker.dittnav.eventaggregator.oppgave

import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.eventaggregator.common.exceptions.FieldNullException
import no.nav.personbruker.dittnav.eventaggregator.nokkel.createNokkel
import org.amshove.kluent.*
import org.junit.jupiter.api.Test
import java.time.ZoneId

class OppgaveTransformerTest {

    @Test
    fun `should transform external to internal`() {
        val eventId = 123
        val external = AvroOppgaveObjectMother.createOppgave(eventId)
        val nokkel = createNokkel(eventId)

        val internal = OppgaveTransformer.toInternal(nokkel, external)

        internal.fodselsnummer `should be equal to` external.getFodselsnummer()
        internal.grupperingsId `should be equal to` external.getGrupperingsId()
        internal.eventId `should be equal to` nokkel.getEventId()
        internal.link `should be equal to` external.getLink()
        internal.tekst `should be equal to` external.getTekst()
        internal.produsent `should be equal to` nokkel.getSystembruker()
        internal.sikkerhetsnivaa `should be equal to` external.getSikkerhetsnivaa()

        val transformedEventTidspunktAsLong = internal.eventTidspunkt.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
        transformedEventTidspunktAsLong `should be equal to` external.getTidspunkt()

        internal.aktiv `should be` true
        internal.sistOppdatert.`should not be null`()
        internal.id.`should be null`()
    }

    @Test
    fun `should throw FieldNullException when fodselsnummer is empty`() {
        val fodselsnummer = ""
        val eventId = 123
        val event = AvroOppgaveObjectMother.createOppgaveWithFodselsnummer(eventId, fodselsnummer)
        val nokkel = createNokkel(eventId)

        invoking {
            runBlocking {
                OppgaveTransformer.toInternal(nokkel, event)
            }
        } `should throw` FieldNullException::class
    }
}
