package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import no.nav.brukernotifikasjon.schemas.Beskjed
import java.time.Instant

object AvroBeskjedObjectMother {

    private val defaultLopenummer = 1
    private val defaultFodselsnr = "12345"
    private val defaultText = "Dette er Beskjed til brukeren"
    private val defaultEksternVarsling = false

    fun createBeskjed(lopenummer: Int): Beskjed {
        return createBeskjed(lopenummer, defaultFodselsnr, defaultText)
    }

    fun createBeskjed(lopenummer: Int, fodselsnummer: String, text: String): Beskjed {
        return Beskjed(
                Instant.now().toEpochMilli(),
                Instant.now().toEpochMilli(),
                fodselsnummer,
                "100$lopenummer",
                text,
                "https://nav.no/systemX/$lopenummer",
                4,
                defaultEksternVarsling,
                emptyList()
        )
    }

    fun createBeskjedWithoutSynligFremTilSatt(): Beskjed {
        return Beskjed(
                Instant.now().toEpochMilli(),
                null,
                defaultFodselsnr,
                "100$defaultLopenummer",
                defaultText,
                "https://nav.no/systemX/$defaultLopenummer",
                4,
                defaultEksternVarsling,
                emptyList()
        )
    }
}
