package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import no.nav.brukernotifikasjon.schemas.internal.DoneIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroDoneInternObjectMother {

    private val defaultGrupperingsId = "123"

    fun createDoneIntern(): DoneIntern {
        return DoneIntern(
            createULID(),
            Instant.now().toEpochMilli(),
            defaultGrupperingsId
        )
    }
}
