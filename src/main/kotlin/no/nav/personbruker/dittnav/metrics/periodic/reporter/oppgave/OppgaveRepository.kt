package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.BrukernotifikasjonRepository
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.persistEachIndividuallyAndAggregateResults
import org.slf4j.LoggerFactory

class OppgaveRepository(private val database: Database) : BrukernotifikasjonRepository<Oppgave> {

    private val log = LoggerFactory.getLogger(OppgaveRepository::class.java)

    override suspend fun createInOneBatch(entities: List<Oppgave>): ListPersistActionResult<Oppgave> {
        return database.queryWithExceptionTranslation {
            createOppgaver(entities)
        }
    }

    override suspend fun createOneByOneToFilterOutTheProblematicEvents(entities: List<Oppgave>): ListPersistActionResult<Oppgave> {
        return database.queryWithExceptionTranslation {
            entities.persistEachIndividuallyAndAggregateResults { entity ->
                createOppgave(entity)
            }
        }
    }

}
