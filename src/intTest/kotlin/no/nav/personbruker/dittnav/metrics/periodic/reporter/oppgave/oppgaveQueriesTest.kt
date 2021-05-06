package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.H2Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEvents
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEventsByActiveStatus
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class oppgaveQueriesTest {

    private val database = H2Database()

    private val fodselsnummer1 = "12345"
    private val fodselsnummer2 = "54321"

    private val oppgave1: Oppgave
    private val oppgave2: Oppgave
    private val oppgave3: Oppgave

    private val allEvents: List<Oppgave>

    init {
        oppgave1 = createOppgave("1", fodselsnummer1)
        oppgave2 = createOppgave("2", fodselsnummer2)
        oppgave3 = createOppgave("3", fodselsnummer1)
        allEvents = listOf(oppgave1, oppgave2, oppgave3)
    }

    private fun createOppgave(eventId: String, fodselsnummer: String): Oppgave {
        var oppgave = OppgaveObjectMother.giveMeAktivOppgave(eventId, fodselsnummer)
        runBlocking {
            database.dbQuery {
                val generatedId = createOppgave(oppgave).entityId
                oppgave = oppgave.copy(id = generatedId)
            }
        }
        return oppgave
    }

    @AfterAll
    fun tearDown() {
        runBlocking {
            database.dbQuery { deleteAllOppgave() }
        }
    }

    @Test
    fun `Skal telle det totale antall oppgaver`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEvents(EventType.OPPGAVE)
            }
        } `should be equal to` allEvents.size.toLong()
    }

    @Test
    fun `Skal telle det totale antall aktive oppgaver`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEventsByActiveStatus(EventType.OPPGAVE, true)
            }
        } `should be equal to` allEvents.size.toLong()
    }

    @Test
    fun `Skal telle det totale antall inaktive oppgaver`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEventsByActiveStatus(EventType.OPPGAVE, false)
            }
        } `should be equal to` 0
    }

}
