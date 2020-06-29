package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.isOtherEnvironmentThanProd
import org.slf4j.LoggerFactory

class DbEventCounterService(private val metricsProbe: DbCountingMetricsProbe,
                            private val repository: MetricsRepository) {

    private val log = LoggerFactory.getLogger(DbEventCounterService::class.java)

    suspend fun countEventsAndReportMetrics() = withContext(Dispatchers.IO) {
        val beskjeder = async {
            countAndReportMetricsForBeskjeder()
        }
        val innboks = async {
            countAndReportMetricsForInnboksEventer()
        }
        val oppgave = async {
            countAndReportMetricsForOppgaver()
        }
        val done = async {
            countAndReportMetricsForDoneEvents()
        }

        beskjeder.await()
        innboks.await()
        oppgave.await()
        done.await()
    }

    private suspend fun countAndReportMetricsForBeskjeder() {
        try {
            metricsProbe.runWithMetrics(EventType.BESKJED) {
                val grupperPerProdusent = repository.getNumberOfBeskjedEventsGroupedByProdusent()
                addEventsByProducer(grupperPerProdusent)
            }

        } catch (e: Exception) {
            log.warn("Klarte ikke å telle og rapportere metrics for antall beskjed-eventer i cache-en", e)
        }
    }

    private suspend fun countAndReportMetricsForInnboksEventer() {
        if (isOtherEnvironmentThanProd()) {
            try {
                metricsProbe.runWithMetrics(EventType.INNBOKS) {
                    val grupperPerProdusent = repository.getNumberOfInnboksEventsGroupedByProdusent()
                    addEventsByProducer(grupperPerProdusent)
                }

            } catch (e: Exception) {
                log.warn("Klarte ikke å telle og rapportere metrics for antall innboks-eventer i cache-en", e)
            }
        }
    }

    private suspend fun countAndReportMetricsForOppgaver() {
        try {
            metricsProbe.runWithMetrics(EventType.OPPGAVE) {
                val grupperPerProdusent = repository.getNumberOfOppgaveEventsGroupedByProdusent()
                addEventsByProducer(grupperPerProdusent)
            }

        } catch (e: Exception) {
            log.warn("Klarte ikke å telle og rapportere metrics for antall oppgave-eventer i cache-en", e)
        }
    }

    private suspend fun countAndReportMetricsForDoneEvents() {
        try {
            metricsProbe.runWithMetrics(EventType.DONE) {
                addEventsByProducer(repository.getNumberOfDoneEventsInWaitingTableGroupedByProdusent())
                addEventsByProducer(repository.getNumberOfInactiveBrukernotifikasjonerGroupedByProdusent())
            }

        } catch (e: Exception) {
            log.warn("Klarte ikke å telle og rapportere metrics for antall done-eventer i cache-en", e)
        }
    }

}
