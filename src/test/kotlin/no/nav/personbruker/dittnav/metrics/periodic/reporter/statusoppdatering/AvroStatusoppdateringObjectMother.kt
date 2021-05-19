package no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering

import no.nav.brukernotifikasjon.schemas.Statusoppdatering
import java.time.Instant

object AvroStatusoppdateringObjectMother {

    private val defaultLopenummer = 1
    private val defaultFodselsnr = "12345"
    private val defaultLink = "http://dummyLink"
    private val defaultSikkerhetsnivaa = 4
    private val defaultStatusIntern = "statusIntern"
    private val defaultSakstema = "dummySakstema"

    fun createStatusoppdateringWithStatusGlobal(statusGlobal: String): Statusoppdatering {
        return createStatusoppdatering(defaultLopenummer, defaultFodselsnr, statusGlobal, defaultStatusIntern, defaultSakstema)
    }

    fun createStatusoppdatering(lopenummer: Int, fodselsnummer: String, statusGlobal: String, statusIntern: String?, sakstema: String): Statusoppdatering {
        return Statusoppdatering(
            Instant.now().toEpochMilli(),
            "100$lopenummer",
            defaultLink,
            defaultSikkerhetsnivaa,
            statusGlobal,
            statusIntern,
            sakstema,
            fodselsnummer)
    }

}
