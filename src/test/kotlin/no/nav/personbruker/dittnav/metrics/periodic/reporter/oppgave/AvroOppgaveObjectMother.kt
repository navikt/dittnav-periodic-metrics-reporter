package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import no.nav.brukernotifikasjon.schemas.Oppgave
import java.time.Instant

object AvroOppgaveObjectMother {

    private val defaultLopenummer = 1
    private val defaultFodselsnr = "12345"
    private val defaultEksternVarsling = false

    fun createOppgave(tekst: String): Oppgave {
        return createOppgave(defaultLopenummer, defaultFodselsnr, tekst)
    }

    fun createOppgave(lopenummer: Int, fodselsnummer: String, tekst: String): Oppgave {
        return Oppgave(
                Instant.now().toEpochMilli(),
                fodselsnummer,
                "100$lopenummer",
                tekst,
                "https://nav.no/systemX/$lopenummer",
                4,
                defaultEksternVarsling)
    }

}
