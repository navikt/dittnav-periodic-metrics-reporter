package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MetricsRepository(private val database: Database) {

    suspend fun getNumberOfBeskjedEventsGroupedByProdusent(): Map<String, Int> {
        return database.queryWithExceptionTranslation {
            countTotalNumberOfEventsGroupedBySystembruker(EventType.BESKJED)
        }
    }

    suspend fun getNumberOfInnboksEventsGroupedByProdusent(): Map<String, Int> {
        return database.queryWithExceptionTranslation {
            countTotalNumberOfEventsGroupedBySystembruker(EventType.INNBOKS)
        }
    }

    suspend fun getNumberOfStatusoppdateringEventsGroupedByProdusent(): Map<String, Int> {
        return database.queryWithExceptionTranslation {
            countTotalNumberOfEventsGroupedBySystembruker(EventType.STATUSOPPDATERING)
        }
    }

    suspend fun getNumberOfOppgaveEventsGroupedByProdusent(): Map<String, Int> {
        return database.queryWithExceptionTranslation {
            countTotalNumberOfEventsGroupedBySystembruker(EventType.OPPGAVE)
        }
    }

    suspend fun getNumberOfDoneEventsInWaitingTableGroupedByProdusent(): Map<String, Int> {
        return database.queryWithExceptionTranslation {
            countTotalNumberOfEventsGroupedBySystembruker(EventType.DONE)
        }
    }

    suspend fun getNumberOfInactiveBrukernotifikasjonerGroupedByProdusent(): Map<String, Int> {
        return database.queryWithExceptionTranslation {
            countTotalNumberOfBrukernotifikasjonerByActiveStatus(false)
        }
    }
}
