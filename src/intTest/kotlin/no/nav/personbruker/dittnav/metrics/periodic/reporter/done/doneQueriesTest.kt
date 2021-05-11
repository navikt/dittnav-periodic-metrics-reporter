package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.H2Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEvents
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class doneQueriesTest {

    private val database = H2Database()
    private val done1 = DoneObjectMother.giveMeDone("1")
    private val done2 = DoneObjectMother.giveMeDone("2")
    private val done3 = DoneObjectMother.giveMeDone("3")
    private val allEvents = listOf(done1, done2, done3)

    init {
        runBlocking {
            database.dbQuery {
                createDoneEvents(listOf(done1, done2, done3))
            }
        }
    }

    @AfterAll
    fun tearDown() {
        runBlocking {
            database.dbQuery { deleteAllDone() }
        }
    }

    @Test
    fun `Skal telle det totale antall done-eventer`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEvents(EventType.DONE)
            }
        } `should be equal to` allEvents.size.toLong()
    }
}
