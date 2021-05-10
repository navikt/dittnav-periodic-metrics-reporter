package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.H2Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEvents
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEventsByActiveStatus
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class beskjedQueriesTest {

    private val database = H2Database()

    private val beskjed1: Beskjed
    private val beskjed2: Beskjed
    private val beskjed3: Beskjed
    private val beskjed4: Beskjed

    private val allEvents: List<Beskjed>

    init {
        beskjed1 = createBeskjed("1", "12345")
        beskjed2 = createBeskjed("2", "12345")
        beskjed3 = createBeskjed("3", "12345")
        beskjed4 = createBeskjed("4", "6789")
        allEvents = listOf(beskjed1, beskjed2, beskjed3, beskjed4)
    }

    private fun createBeskjed(eventId: String, fodselsnummer: String): Beskjed {
        val beskjed = BeskjedObjectMother.giveMeAktivBeskjed(eventId, fodselsnummer)
        return runBlocking {
            database.dbQuery {
                createBeskjed(beskjed).entityId.let {
                    beskjed.copy(id = it)
                }
            }
        }
    }

    @AfterAll
    fun tearDown() {
        runBlocking {
            database.dbQuery { deleteAllBeskjed() }
        }
    }

    @Test
    fun `Skal telle det totale antall beskjeder`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEvents(EventType.BESKJED)
            }
        } `should be equal to` allEvents.size.toLong()
    }

    @Test
    fun `Skal telle det totale antall aktive beskjeder`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEventsByActiveStatus(EventType.BESKJED, true)
            }
        } `should be equal to` allEvents.size.toLong()
    }

    @Test
    fun `Skal telle det totale antall inaktive beskjeder`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEventsByActiveStatus(EventType.BESKJED, false)
            }
        } `should be equal to` 0
    }

}
