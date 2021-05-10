package no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering

import createStatusoppdatering
import deleteAllStatusoppdatering
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.H2Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.countTotalNumberOfEvents
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class statusoppdateringQueriesTest {

    private val database = H2Database()

    private val fodselsnummer1 = "12345678901"
    private val fodselsnummer2 = "10987654321"

    private val statusoppdatering1: Statusoppdatering
    private val statusoppdatering2: Statusoppdatering
    private val statusoppdatering3: Statusoppdatering

    private val allEvents: List<Statusoppdatering>

    init {
        statusoppdatering1 = createStatusoppdatering("1", fodselsnummer1)
        statusoppdatering2 = createStatusoppdatering("2",  fodselsnummer2)
        statusoppdatering3 = createStatusoppdatering("3", fodselsnummer1)
        allEvents = listOf(statusoppdatering1, statusoppdatering2, statusoppdatering3)
    }

    private fun createStatusoppdatering(eventId: String, fodselsnummer: String): Statusoppdatering {
        var statusoppdatering = StatusoppdateringObjectMother.giveMeStatusoppdatering(eventId, fodselsnummer)
        runBlocking {
            database.dbQuery {
                val generatedId = createStatusoppdatering(statusoppdatering).entityId
                statusoppdatering = statusoppdatering.copy(id = generatedId)
            }
        }
        return statusoppdatering
    }

    @AfterAll
    fun tearDown() {
        runBlocking {
            database.dbQuery { deleteAllStatusoppdatering() }
        }
    }

    @Test
    fun `Skal telle det totale antall statusoppdateringer`() {
        runBlocking {
            database.dbQuery {
                countTotalNumberOfEvents(EventType.STATUSOPPDATERING)
            }
        } `should be equal to` allEvents.size.toLong()
    }
}
