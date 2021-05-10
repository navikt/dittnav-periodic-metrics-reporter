package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import org.amshove.kluent.`should contain`
import org.junit.jupiter.api.Test

internal class DoneTest {

    @Test
    fun `skal returnere maskerte data fra toString-metoden`() {
        val done = DoneObjectMother.giveMeDone("dummyEventId", "dummProdusent", "12345678901")
        val doneAsString = done.toString()
        doneAsString `should contain` "fodselsnummer=***"
    }

}
