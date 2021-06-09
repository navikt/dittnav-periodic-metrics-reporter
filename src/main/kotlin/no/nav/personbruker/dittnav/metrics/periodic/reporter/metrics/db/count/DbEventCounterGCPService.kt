package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions
import org.slf4j.LoggerFactory

class DbEventCounterGCPService(
    private val metricsProbe: DbCountingMetricsProbe,
    private val repository: MetricsRepository
) {

    private val log = LoggerFactory.getLogger(DbEventCounterGCPService::class.java)

    suspend fun countAllEventTypesAsync() : CountingMetricsSessions = withContext(Dispatchers.IO) {
        val beskjeder = async {
            countBeskjeder()
        }
        val innboks = async {
            countInnboksEventer()
        }
        val oppgave = async {
            countOppgaver()
        }
        val statusoppdatering = async {
            countStatusoppdateringer()
        }
        val done = async {
            countDoneEvents()
        }
        val feilrespons = async {
            countFeilrespons()
        }

        val sessions = CountingMetricsSessions()
        sessions.put(EventType.BESKJED_INTERN, beskjeder.await())
        sessions.put(EventType.DONE_INTERN, done.await())
        sessions.put(EventType.INNBOKS_INTERN, innboks.await())
        sessions.put(EventType.OPPGAVE_INTERN, oppgave.await())
        sessions.put(EventType.STATUSOPPDATERING_INTERN, statusoppdatering.await())
        sessions.put(EventType.FEILRESPONS, feilrespons.await())
        return@withContext sessions
    }

    fun countBeskjeder(): DbCountingMetricsSession {
        return DbCountingMetricsSession(EventType.BESKJED_INTERN)
    }

    fun countInnboksEventer(): DbCountingMetricsSession {
        return DbCountingMetricsSession(EventType.INNBOKS_INTERN)
    }

    fun countStatusoppdateringer(): DbCountingMetricsSession {
        return DbCountingMetricsSession(EventType.STATUSOPPDATERING_INTERN)
    }

    fun countOppgaver(): DbCountingMetricsSession {
        return DbCountingMetricsSession(EventType.OPPGAVE_INTERN)
    }

    fun countDoneEvents(): DbCountingMetricsSession {
        return DbCountingMetricsSession(EventType.DONE_INTERN)
    }

    fun countFeilrespons(): DbCountingMetricsSession {
        return DbCountingMetricsSession(EventType.FEILRESPONS)
    }
}
