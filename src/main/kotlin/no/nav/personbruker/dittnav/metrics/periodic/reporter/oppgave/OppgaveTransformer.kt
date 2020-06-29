package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.validation.validateNonNullField
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

object OppgaveTransformer {

    private const val newRecordsAreActiveByDefault = true

    fun toInternal(nokkel: Nokkel, external: no.nav.brukernotifikasjon.schemas.Oppgave): Oppgave {
        return Oppgave(
                nokkel.getSystembruker(),
                nokkel.getEventId(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(external.getTidspunkt()), ZoneId.of("UTC")),
                validateNonNullField(external.getFodselsnummer(), "Fødselsnummer"),
                external.getGrupperingsId(),
                external.getTekst(),
                external.getLink(),
                external.getSikkerhetsnivaa(),
                LocalDateTime.now(ZoneId.of("UTC")),
                newRecordsAreActiveByDefault
        )
    }
}