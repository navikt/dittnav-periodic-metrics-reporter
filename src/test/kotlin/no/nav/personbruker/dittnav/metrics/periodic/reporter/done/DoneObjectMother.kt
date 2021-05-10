package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import java.time.LocalDateTime
import java.time.ZoneId

object DoneObjectMother {

    fun giveMeDone(eventId: String): Done {
        val systembruker = "dummySystembruker"
        val fodselsnummer = "12345678901"
        return giveMeDone(eventId, systembruker, fodselsnummer)
    }

    fun giveMeDone(eventId: String, systembruker: String): Done {
        val fodselsnummer = "12345678901"
        return giveMeDone(eventId, systembruker, fodselsnummer)
    }

    fun giveMeDone(eventId: String, systembruker: String, fodselsnummer: String): Done {
        return Done(
                systembruker,
                eventId,
                LocalDateTime.now(ZoneId.of("UTC")),
                fodselsnummer,
                "100${eventId}"
        )
    }

}
