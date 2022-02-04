package no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering

import no.nav.brukernotifikasjon.schemas.internal.StatusoppdateringIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroStatusoppdateringInternObjectMother {

    private val defaultLink = "https://nav.no/systemX/"
    private val defaultStatusGlobal = "statusGlobal"
    private val defaultStatusIntern = "statusIntern"
    private val defaultSakstema = "sakstema"

    fun createStatusoppdateringIntern(): StatusoppdateringIntern {
        return StatusoppdateringIntern(
            Instant.now().toEpochMilli(),
            defaultLink,
            4,
            defaultStatusGlobal,
            defaultStatusIntern,
            defaultSakstema
        )
    }
}
