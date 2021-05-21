package no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks

import no.nav.brukernotifikasjon.schemas.internal.InnboksIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroInnboksInternObjectMother {

    private val defaultGrupperingsId = "123"
    private val defaultText = "Dette er tekst til brukeren"
    private val defaultLink = "https://nav.no/systemX/"

    fun createInnboksIntern(): InnboksIntern {
        return InnboksIntern(
            createULID(),
            Instant.now().toEpochMilli(),
            defaultGrupperingsId,
            defaultText,
            defaultLink,
            4
        )
    }
}
