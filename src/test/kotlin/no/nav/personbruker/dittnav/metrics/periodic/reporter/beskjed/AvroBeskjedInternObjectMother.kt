package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import no.nav.brukernotifikasjon.schemas.internal.BeskjedIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroBeskjedInternObjectMother {

    private val defaultUlid = createULID()
    private val defaultGrupperingsId = "123"
    private val defaultText = "Dette er Beskjed til brukeren"
    private val defaultEksternVarsling = false


    fun createBeskjedIntern(): BeskjedIntern {
        return BeskjedIntern(
            defaultUlid,
            Instant.now().toEpochMilli(),
            Instant.now().toEpochMilli(),
            defaultGrupperingsId,
            defaultText,
            "https://nav.no/systemX/",
            4,
            defaultEksternVarsling
        )
    }
}
