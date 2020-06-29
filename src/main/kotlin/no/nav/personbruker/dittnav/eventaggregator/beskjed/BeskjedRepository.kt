package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.BrukernotifikasjonRepository
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.persistEachIndividuallyAndAggregateResults
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BeskjedRepository(private val database: Database) : BrukernotifikasjonRepository<Beskjed> {

    private val log: Logger = LoggerFactory.getLogger(BeskjedRepository::class.java)

    override suspend fun createInOneBatch(entities: List<Beskjed>): ListPersistActionResult<Beskjed> {
        return database.queryWithExceptionTranslation {
            createBeskjeder(entities)
        }
    }

    override suspend fun createOneByOneToFilterOutTheProblematicEvents(entities: List<Beskjed>): ListPersistActionResult<Beskjed> {
        return database.queryWithExceptionTranslation {
            entities.persistEachIndividuallyAndAggregateResults { entity ->
                createBeskjed(entity)
            }
        }
    }

}
