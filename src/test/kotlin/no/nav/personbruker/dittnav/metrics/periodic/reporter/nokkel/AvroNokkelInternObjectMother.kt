package no.nav.personbruker.dittnav.metrics.periodic.reporter.nokkel

import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID

object AvroNokkelInternObjectMother {
    private val defaultFodselsnummer = "12345678901"
    private val defaultGruperingsid = "123"

    fun createNokkelIntern(eventId: Int): NokkelIntern =
        NokkelIntern(createULID(), eventId.toString(), defaultGruperingsid, defaultFodselsnummer, "ns", "app", "dummySystembruker")
}
