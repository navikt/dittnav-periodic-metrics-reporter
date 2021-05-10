package no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks

import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.H2Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEvents
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEventsByActiveStatus
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class innboksQueriesTest {

    private val database = H2Database()

    private val fodselsnummer1 = "12345"
    private val fodselsnummer2 = "67890"

    private val innboks1: Innboks
    private val innboks2: Innboks
    private val innboks3: Innboks

    private val allEvents: List<Innboks>

    init {
        innboks1 = createInnboks("1", fodselsnummer1)
        innboks2 = createInnboks("2", fodselsnummer2)
        innboks3 = createInnboks("3", fodselsnummer1)

        allEvents = listOf(innboks1, innboks2, innboks3)
    }

    private fun createInnboks(eventId: String, fodselsnummer: String): Innboks {
        val innboks = InnboksObjectMother.giveMeAktivInnboks(eventId, fodselsnummer)

        return runBlocking {
            database.dbQuery {
                createInnboks(innboks).entityId.let {
                    innboks.copy(id = it)
                }
            }
        }
    }

    @AfterAll
    fun tearDown() {
        runBlocking {
            database.dbQuery {
                deleteAllInnboks()
            }
        }
    }


    @Test
    fun `Skal telle det totale antall innboks-eventer`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEvents(EventType.INNBOKS)
            }
        } `should be equal to` allEvents.size.toLong()
    }

    @Test
    fun `Skal telle det totale antall aktive innboks-eventer`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEventsByActiveStatus(EventType.INNBOKS, true)
            }
        } `should be equal to` allEvents.size.toLong()
    }

    @Test
    fun `Skal telle det totale antall inaktive innboks-eventer`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEventsByActiveStatus(EventType.INNBOKS, false)
            }
        } `should be equal to` 0
    }
}
