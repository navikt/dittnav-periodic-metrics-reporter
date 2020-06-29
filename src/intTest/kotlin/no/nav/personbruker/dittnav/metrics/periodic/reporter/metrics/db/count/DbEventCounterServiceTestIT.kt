package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import io.mockk.coEvery
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.Beskjed
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.BeskjedObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.createBeskjeder
import no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed.deleteAllBeskjed
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.H2Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.Done
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.DoneObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.createDoneEvents
import no.nav.personbruker.dittnav.metrics.periodic.reporter.done.deleteAllDone
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.Innboks
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.InnboksObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.createInnboksEventer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks.deleteAllInnboks
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.Oppgave
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.OppgaveObjectMother
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.createOppgaver
import no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave.deleteAllOppgave
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

internal class DbEventCounterServiceTestIT {

    private val database = H2Database()
    private val repository = MetricsRepository(database)

    @AfterEach
    fun cleanUp() {
        runBlocking {
            database.dbQuery {
                deleteAllBeskjed()
                deleteAllInnboks()
                deleteAllOppgave()
                deleteAllDone()
            }
        }
    }

    @Test
    fun `Should count beskjed events`() {
        val beskjeder = createBeskjedEventer()
        val metricsProbe = mockk<DbCountingMetricsProbe>(relaxed = true)
        val metricsSession = initMetricsSession(metricsProbe, EventType.BESKJED)
        val service = DbEventCounterService(metricsProbe, repository)

        runBlocking {
            service.countEventsAndReportMetrics()
        }

        metricsSession.getTotalNumber() `should be equal to` 2
        metricsSession.getProducers().size `should be equal to` 2
        metricsSession.getNumberOfEventsFor(beskjeder[0].systembruker) `should be equal to` 1
        metricsSession.getNumberOfEventsFor(beskjeder[1].systembruker) `should be equal to` 1
    }


    @Test
    fun `Should count innboks events`() {
        val innboksEventer = createInnboksEventer()
        val metricsProbe = mockk<DbCountingMetricsProbe>(relaxed = true)
        val metricsSession = initMetricsSession(metricsProbe, EventType.INNBOKS)
        val service = DbEventCounterService(metricsProbe, repository)

        runBlocking {
            service.countEventsAndReportMetrics()
        }

        metricsSession.getTotalNumber() `should be equal to` 2
        metricsSession.getProducers().size `should be equal to` 2
        metricsSession.getNumberOfEventsFor(innboksEventer[0].systembruker) `should be equal to` 1
        metricsSession.getNumberOfEventsFor(innboksEventer[1].systembruker) `should be equal to` 1
    }

    @Test
    fun `Should count oppgave events`() {
        val oppgaver = createOppgaveEventer()
        val metricsProbe = mockk<DbCountingMetricsProbe>(relaxed = true)
        val metricsSession = initMetricsSession(metricsProbe, EventType.OPPGAVE)
        val service = DbEventCounterService(metricsProbe, repository)

        runBlocking {
            service.countEventsAndReportMetrics()
        }

        metricsSession.getTotalNumber() `should be equal to` 2
        metricsSession.getProducers().size `should be equal to` 2
        metricsSession.getNumberOfEventsFor(oppgaver[0].systembruker) `should be equal to` 1
        metricsSession.getNumberOfEventsFor(oppgaver[1].systembruker) `should be equal to` 1
    }

    @Test
    fun `Should count done events from the wait table, and include brukernotifikasjoner marked as inactive (done)`() {
        createBeskjedEventer()
        createInnboksEventer()
        createOppgaveEventer()
        val doneEventer = createDoneEventInWaitingTable()

        val metricsProbe = mockk<DbCountingMetricsProbe>(relaxed = true)
        val metricsSession = initMetricsSession(metricsProbe, EventType.DONE)
        val service = DbEventCounterService(metricsProbe, repository)

        runBlocking {
            service.countEventsAndReportMetrics()
        }

        metricsSession.getTotalNumber() `should be equal to` 4
        metricsSession.getProducers().size `should be equal to` 2
        metricsSession.getNumberOfEventsFor("dummySystembruker") `should be equal to` 3
        metricsSession.getNumberOfEventsFor(doneEventer[0].systembruker) `should be equal to` 1
    }

    private fun initMetricsSession(metricsProbe: DbCountingMetricsProbe, eventType: EventType): DbCountingMetricsSession {
        val metricsSession = DbCountingMetricsSession(eventType)
        `Sorg for at metrics session trigges`(metricsProbe, metricsSession, eventType)
        return metricsSession
    }

    private fun `Sorg for at metrics session trigges`(metricsProbe: DbCountingMetricsProbe, metricsSession: DbCountingMetricsSession, eventType: EventType) {
        val slot = slot<suspend DbCountingMetricsSession.() -> Unit>()
        coEvery { metricsProbe.runWithMetrics(eventType, capture(slot)) } coAnswers {
            slot.captured.invoke(metricsSession)
        }
    }

    private fun createBeskjedEventer(): List<Beskjed> {
        val beskjeder = listOf(
                BeskjedObjectMother.giveMeAktivBeskjed("321", "567", "systembrukerB"),
                BeskjedObjectMother.giveMeInaktivBeskjed()
        )

        runBlocking {
            database.dbQuery {
                createBeskjeder(beskjeder)
            }
        }
        return beskjeder
    }

    private fun createInnboksEventer(): List<Innboks> {
        val innboksEventer = listOf(
                InnboksObjectMother.giveMeAktivInnboks("213", "678", "systembrukerI"),
                InnboksObjectMother.giveMeInaktivInnboks()
        )
        runBlocking {
            database.dbQuery {
                createInnboksEventer(innboksEventer)
            }
        }
        return innboksEventer
    }

    private fun createOppgaveEventer(): List<Oppgave> {
        val oppgaver = listOf(
                OppgaveObjectMother.giveMeAktivOppgave("132", "789", "systembrukerO"),
                OppgaveObjectMother.giveMeInaktivOppgave()
        )
        runBlocking {
            database.dbQuery {
                createOppgaver(oppgaver)
            }
        }
        return oppgaver
    }

    private fun createDoneEventInWaitingTable(): List<Done> {
        val doneEventer = listOf(
                DoneObjectMother.giveMeDone("e-2", "systembrukerD")
        )
        runBlocking {
            database.dbQuery {
                createDoneEvents(doneEventer)
            }
        }
        return doneEventer
    }

}
