package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.MetricsReportingException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSession
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterGCPService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsSession
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterOnPremService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterAivenService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterOnPremService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsSession
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MetricsSubmitterService(
    private val dbEventCounterOnPremService: DbEventCounterOnPremService,
    private val dbEventCounterGCPService: DbEventCounterGCPService,
    private val topicEventCounterServiceOnPrem: TopicEventCounterOnPremService<Nokkel>,
    private val topicEventCounterServiceAiven: TopicEventCounterAivenService<NokkelIntern>,
    private val dbMetricsReporter: DbMetricsReporter,
    private val kafkaMetricsReporter: TopicMetricsReporter
) {

    private val log: Logger = LoggerFactory.getLogger(MetricsSubmitterService::class.java)

    private val lastReportedUniqueKafkaEvents = HashMap<EventType, Int>()

    suspend fun submitMetrics() {
        try {
            val topicSessionsOnPrem = topicEventCounterServiceOnPrem.countAllEventTypesAsync()
            val topicSessionsAiven = topicEventCounterServiceAiven.countAllEventTypesAsync()
            val dbSessionsOnPrem = dbEventCounterOnPremService.countAllEventTypesAsync()
            val dbSessionsAiven = dbEventCounterGCPService.countAllEventTypesAsync()

            val sessionComparatorOnPrem = SessionComparator(topicSessionsOnPrem, dbSessionsOnPrem)
            val sessionComparatorAiven = SessionComparator(topicSessionsAiven, dbSessionsAiven)

            sessionComparatorOnPrem.eventTypesWithSessionFromBothSources().forEach { eventType ->
                reportMetricsByEventType(topicSessionsOnPrem, dbSessionsOnPrem, eventType)
            }

            sessionComparatorAiven.eventTypesWithSessionFromBothSources().forEach { eventType ->
                reportMetricsByEventType(topicSessionsAiven, dbSessionsAiven, eventType)
            }

        } catch (e: CountException) {
            log.warn("Klarte ikke å telle eventer", e)

        } catch (e: MetricsReportingException) {
            log.warn("Klarte ikke å rapportere metrikker", e)

        } catch (e: Exception) {
            log.error("En uventet feil oppstod, klarte ikke å telle og/eller rapportere metrikker.", e)
        }
    }

    private suspend fun reportMetricsByEventType(
            topicSessions: CountingMetricsSessions,
            dbSessions: CountingMetricsSessions,
            eventType: EventType
    ) {
        val kafkaEventSession = topicSessions.getForType(eventType)

        if(EventType.FEILRESPONS == eventType) {
            kafkaMetricsReporter.report(kafkaEventSession as TopicMetricsSession)
            lastReportedUniqueKafkaEvents[eventType] = kafkaEventSession.getNumberOfUniqueEvents()
        } else if (countedMoreKafkaEventsThanLastCount(kafkaEventSession, eventType)) {
            val dbSession = dbSessions.getForType(eventType)
            dbMetricsReporter.report(dbSession as DbCountingMetricsSession)
            kafkaMetricsReporter.report(kafkaEventSession as TopicMetricsSession)
            lastReportedUniqueKafkaEvents[eventType] = kafkaEventSession.getNumberOfUniqueEvents()
        }  else if (!currentAndLastCountWasZero(kafkaEventSession, eventType)) {
            val currentCount = kafkaEventSession.getNumberOfUniqueEvents()
            val previousCount = lastReportedUniqueKafkaEvents.getOrDefault(eventType, 0)
            val msg = "Det har oppstått en tellefeil, rapporterer derfor ikke nye $eventType-metrikker. " +
                    "Antall unike eventer ved forrige rapportering $previousCount, antall telt nå $currentCount."
            log.warn(msg)
        }
    }

    private fun countedMoreKafkaEventsThanLastCount(session: CountingMetricsSession, eventType: EventType): Boolean {
        val currentCount = session.getNumberOfUniqueEvents()
        return currentCount > 0 && currentCount >= lastReportedUniqueKafkaEvents.getOrDefault(eventType, 0)
    }

    private fun currentAndLastCountWasZero(session: CountingMetricsSession, eventType: EventType): Boolean {
        val currentCount = session.getNumberOfUniqueEvents()
        return currentCount == 0 && lastReportedUniqueKafkaEvents.getOrDefault(eventType, 0) == 0
    }
}
