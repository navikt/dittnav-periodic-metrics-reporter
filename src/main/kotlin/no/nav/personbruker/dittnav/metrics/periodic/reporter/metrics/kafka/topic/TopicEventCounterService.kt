package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.foundRecords
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.resetTheGroupIdsOffsetToZero
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.isOtherEnvironmentThanProd
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit

class TopicEventCounterService (
    val beskjedCounter: TopicEventTypeCounter,
    val innboksCounter: TopicEventTypeCounter,
    val oppgaveCounter: TopicEventTypeCounter,
    val doneCounter: TopicEventTypeCounter
) {

    suspend fun countAllEventTypesAsync(): CountingMetricsSessions = coroutineScope {

        val beskjeder = beskjedCounter.countEventsAsync()
        val oppgaver = oppgaveCounter.countEventsAsync()
        val done = doneCounter.countEventsAsync()

        val innboks = if (isOtherEnvironmentThanProd()) {
            innboksCounter.countEventsAsync()
        } else {
            async { TopicMetricsSession(EventType.INNBOKS) }
        }

        val sessions = CountingMetricsSessions()

        sessions.put(EventType.BESKJED, beskjeder.await())
        sessions.put(EventType.DONE, done.await())
        sessions.put(EventType.INNBOKS, innboks.await())
        sessions.put(EventType.OPPGAVE, oppgaver.await())

        sessions
    }
}
