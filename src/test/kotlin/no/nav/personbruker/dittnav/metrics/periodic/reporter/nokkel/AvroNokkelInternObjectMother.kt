package no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel

import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern

object AvroNokkelInternObjectMother {
    private val defaultFodselsnummer = "12345678901"

    fun createNokkelIntern(eventId: Int): NokkelIntern =
        NokkelIntern("dummySystembruker", eventId.toString(), defaultFodselsnummer)
}
