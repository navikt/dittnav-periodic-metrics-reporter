package no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering

import java.time.LocalDateTime
import java.time.ZoneId

object StatusoppdateringObjectMother {

    fun giveMeeAktivStatusoppdatering(eventId: String, fodselsnummer: String): Statusoppdatering {
        return Statusoppdatering(
            systembruker = "dummySystembruker",
            eventId = eventId,
            eventTidspunkt = LocalDateTime.now(ZoneId.of("UTC")),
            fodselsnummer = fodselsnummer,
            grupperingsId = "Dok12345",
            link = "https://nav.no/systemX/",
            sikkerhetsnivaa = 4,
            sistOppdatert = LocalDateTime.now(ZoneId.of("UTC")),
            statusGlobal = "SENDT",
            statusIntern = "dummyStatusIntern",
            sakstema = "dummySakstema"
        )
    }
}
