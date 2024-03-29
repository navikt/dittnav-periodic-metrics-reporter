package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import no.nav.brukernotifikasjon.schemas.internal.OppgaveIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroOppgaveInternObjectMother {

    private val defaultText = "Dette er en Oppgave til brukeren"
    private val defaultLink = "https://nav.no/systemX/"
    private val defaultEksternVarsling = false

    fun createOppgaveIntern(): OppgaveIntern {
        return OppgaveIntern(
            Instant.now().toEpochMilli(),
            defaultText,
            defaultLink,
            4,
            defaultEksternVarsling,
            emptyList()
        )
    }
}
