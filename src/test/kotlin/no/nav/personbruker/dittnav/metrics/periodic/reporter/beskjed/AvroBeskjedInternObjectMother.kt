package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import no.nav.brukernotifikasjon.schemas.internal.BeskjedIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroBeskjedInternObjectMother {

    private val defaultGrupperingsId = "123"
    private val defaultText = "Dette er Beskjed til brukeren"
    private val defaultLink = "https://nav.no/systemX/"
    private val defaultEksternVarsling = false


    fun createBeskjedIntern(): BeskjedIntern {
        return BeskjedIntern(
            createULID(),
            Instant.now().toEpochMilli(),
            Instant.now().toEpochMilli(),
            defaultGrupperingsId,
            defaultText,
            defaultLink,
            4,
            defaultEksternVarsling
        )
    }
}
