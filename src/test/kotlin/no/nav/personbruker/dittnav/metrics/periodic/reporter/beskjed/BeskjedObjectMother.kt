package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.random.Random

object BeskjedObjectMother {

    fun giveMeAktivBeskjed(eventId: String, fodselsnummer: String): Beskjed {
        val systembruker = "dummySystembruker"
        return giveMeAktivBeskjed(eventId, fodselsnummer, systembruker)
    }

    fun giveMeAktivBeskjed(eventId: String, fodselsnummer: String, systembruker: String): Beskjed {
        return Beskjed(
                uid = Random.nextInt(1, 100).toString(),
                systembruker = systembruker,
                eventTidspunkt = LocalDateTime.now(ZoneId.of("UTC")),
                synligFremTil = LocalDateTime.now(ZoneId.of("UTC")),
                fodselsnummer = fodselsnummer,
                eventId = eventId,
                grupperingsId = "systemA010",
                tekst = "Dette er beskjed til brukeren",
                link = "https://nav.no/systemX/$eventId",
                sistOppdatert = LocalDateTime.now(ZoneId.of("UTC")),
                sikkerhetsnivaa = 4,
                aktiv = true)
    }

    fun giveMeInaktivBeskjed(): Beskjed {
        return Beskjed(
                uid = Random.nextInt(1, 100).toString(),
                systembruker = "dummySystembruker",
                eventTidspunkt = LocalDateTime.now(ZoneId.of("UTC")),
                synligFremTil = LocalDateTime.now(ZoneId.of("UTC")),
                fodselsnummer = "123",
                eventId = "b-2",
                grupperingsId = "65432",
                tekst = "Dette er beskjed til brukeren",
                link = "https://nav.no/systemX/",
                sistOppdatert = LocalDateTime.now(ZoneId.of("UTC")),
                sikkerhetsnivaa = 4,
                aktiv = false)
    }

}
