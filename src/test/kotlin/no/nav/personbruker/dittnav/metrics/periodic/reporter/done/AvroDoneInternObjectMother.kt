package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import no.nav.brukernotifikasjon.schemas.internal.DoneIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.createULID
import java.time.Instant

object AvroDoneInternObjectMother {

    fun createDoneIntern(): DoneIntern {
        return DoneIntern(
            Instant.now().toEpochMilli(),
        )
    }
}
