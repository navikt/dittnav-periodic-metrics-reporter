package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.foundRecords
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.resetTheGroupIdsOffsetToZero
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.isOtherEnvironmentThanProd
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.CountingMetricsSessions
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit

class TopicEventCounterService(
    val topicMetricsProbe: TopicMetricsProbe,
    val beskjedCountConsumer: KafkaConsumer<Nokkel, GenericRecord>,
    val innboksCountConsumer: KafkaConsumer<Nokkel, GenericRecord>,
    val oppgaveCountConsumer: KafkaConsumer<Nokkel, GenericRecord>,
    val doneCountConsumer: KafkaConsumer<Nokkel, GenericRecord>
) {

    private val log = LoggerFactory.getLogger(TopicEventCounterService::class.java)

    suspend fun countAllEventTypesAsync(): CountingMetricsSessions = withContext(Dispatchers.IO) {
        val beskjeder = async {
            countBeskjeder()
        }
        val innboks = async {
            countInnboksEventer()
        }
        val oppgave = async {
            countOppgaver()
        }
        val done = async {
            countDoneEvents()
        }

        val sessions = CountingMetricsSessions()
        sessions.put(EventType.BESKJED, beskjeder.await())
        sessions.put(EventType.DONE, done.await())
        sessions.put(EventType.INNBOKS, innboks.await())
        sessions.put(EventType.OPPGAVE, oppgave.await())
        return@withContext sessions
    }

    suspend fun countBeskjeder(): TopicMetricsSession {
        val eventType = EventType.BESKJED
        return try {
            countAllEventTypesAsync(beskjedCountConsumer, eventType)

        } catch (e: Exception) {
            throw CountException("Klarte ikke å telle antall beskjed-eventer", e)
        }
    }

    suspend fun countInnboksEventer(): TopicMetricsSession {
        val eventType = EventType.INNBOKS
        return if (isOtherEnvironmentThanProd()) {
            try {
                countAllEventTypesAsync(innboksCountConsumer, eventType)

            } catch (e: Exception) {
                throw CountException("Klarte ikke å telle antall innboks-eventer", e)
            }

        } else {
            TopicMetricsSession(eventType)
        }
    }

    suspend fun countOppgaver(): TopicMetricsSession {
        val eventType = EventType.OPPGAVE
        return try {
            countAllEventTypesAsync(oppgaveCountConsumer, eventType)

        } catch (e: Exception) {
            throw CountException("Klarte ikke å telle antall oppgave-eventer", e)
        }
    }

    suspend fun countDoneEvents(): TopicMetricsSession {
        return try {
            countAllEventTypesAsync(doneCountConsumer, EventType.DONE)

        } catch (e: Exception) {
            throw CountException("Klarte ikke å telle antall done-eventer", e)
        }
    }

    private suspend fun countAllEventTypesAsync(
        kafkaConsumer: KafkaConsumer<Nokkel, GenericRecord>,
        eventType: EventType
    ): TopicMetricsSession {
        return topicMetricsProbe.runWithMetrics(eventType) {
            var records = kafkaConsumer.poll(Duration.of(5000, ChronoUnit.MILLIS))
            countBatch(records, this)

            while (records.foundRecords()) {
                records = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS))
                countBatch(records, this)
            }
            kafkaConsumer.resetTheGroupIdsOffsetToZero()
        }
    }

    private fun countBatch(records: ConsumerRecords<Nokkel, GenericRecord>, metricsSession: TopicMetricsSession) {
        records.forEach { record ->
            val event = UniqueKafkaEventIdentifierTransformer.toInternal(record)
            metricsSession.countEvent(event)
        }
    }

}
