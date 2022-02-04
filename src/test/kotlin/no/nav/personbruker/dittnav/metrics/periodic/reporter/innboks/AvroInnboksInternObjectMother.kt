package no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks

import no.nav.brukernotifikasjon.schemas.internal.InnboksIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroInnboksInternObjectMother {

    private val defaultText = "Dette er tekst til brukeren"
    private val defaultLink = "https://nav.no/systemX/"
    private val defaultEksternVarsling = false

    fun createInnboksIntern(): InnboksIntern {
        return InnboksIntern(
            Instant.now().toEpochMilli(),
            defaultText,
            defaultLink,
            4,
            defaultEksternVarsling,
            emptyList()
        )
    }
}
