package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.isOtherEnvironmentThanProd
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions
import org.slf4j.LoggerFactory

class DbEventCounterService(
    private val metricsProbe: DbCountingMetricsProbe,
    private val repository: MetricsRepository
) {

    private val log = LoggerFactory.getLogger(DbEventCounterService::class.java)

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

        val sessions = CountingMetricsSessions()
        sessions.put(EventType.BESKJED, beskjeder.await())
        sessions.put(EventType.DONE, done.await())
        sessions.put(EventType.INNBOKS, innboks.await())
        sessions.put(EventType.OPPGAVE, oppgave.await())
        sessions.put(EventType.STATUSOPPDATERING, statusoppdatering.await())
        return@withContext sessions
    }

    suspend fun countBeskjeder(): DbCountingMetricsSession {
        val eventType = EventType.BESKJED
        return try {
            metricsProbe.runWithMetrics(eventType) {
                val grupperPerProdusent = repository.getNumberOfBeskjedEventsGroupedByProdusent()
                addEventsByProducer(grupperPerProdusent)
            }

        } catch (e: Exception) {
            throw CountException("Klarte ikke å telle antall beskjed-eventer i cache-en", e)
        }
    }

    suspend fun countInnboksEventer(): DbCountingMetricsSession {
        val eventType = EventType.INNBOKS
        return if (isOtherEnvironmentThanProd()) {
            try {
                metricsProbe.runWithMetrics(eventType) {
                    val grupperPerProdusent = repository.getNumberOfInnboksEventsGroupedByProdusent()
                    addEventsByProducer(grupperPerProdusent)
                }

            } catch (e: Exception) {
                throw CountException("Klarte ikke å telle antall innboks-eventer i cache-en", e)
            }
        } else {
            DbCountingMetricsSession(eventType)
        }
    }

    suspend fun countStatusoppdateringer(): DbCountingMetricsSession {
        val eventType = EventType.STATUSOPPDATERING
        return if (isOtherEnvironmentThanProd()) {
            try {
                metricsProbe.runWithMetrics(eventType) {
                    val grupperPerProdusent = repository.getNumberOfStatusoppdateringEventsGroupedByProdusent()
                    addEventsByProducer(grupperPerProdusent)
                }

            } catch (e: Exception) {
                throw CountException("Klarte ikke å telle antall statusoppdatering-eventer i cache-en", e)
            }
        } else {
            DbCountingMetricsSession(eventType)
        }
    }

    suspend fun countOppgaver(): DbCountingMetricsSession {
        val eventType = EventType.OPPGAVE
        return try {
            metricsProbe.runWithMetrics(eventType) {
                val grupperPerProdusent = repository.getNumberOfOppgaveEventsGroupedByProdusent()
                addEventsByProducer(grupperPerProdusent)
            }

        } catch (e: Exception) {
            throw CountException("Klarte ikke å telle antall oppgave-eventer i cache-en", e)
        }
    }

    suspend fun countDoneEvents(): DbCountingMetricsSession {
        val eventType = EventType.DONE
        return try {
            metricsProbe.runWithMetrics(eventType) {
                addEventsByProducer(repository.getNumberOfDoneEventsInWaitingTableGroupedByProdusent())
                addEventsByProducer(repository.getNumberOfInactiveBrukernotifikasjonerGroupedByProdusent())
            }

        } catch (e: Exception) {
            throw CountException("Klarte ikke å telle antall done-eventer i cache-en", e)
        }
    }

}
