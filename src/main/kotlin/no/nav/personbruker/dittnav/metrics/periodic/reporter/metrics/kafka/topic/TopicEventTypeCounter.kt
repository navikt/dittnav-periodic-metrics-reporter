package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.foundRecords
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.resetTheGroupIdsOffsetToZero
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.time.temporal.ChronoUnit

class TopicEventTypeCounter(
        val kafkaConsumer: KafkaConsumer<Nokkel, GenericRecord>,
        val eventType: EventType,
        val deltaCountingEnabled: Boolean
) {

    var previousSession: TopicMetricsSession? = null

    suspend fun countEventsAsync(): Deferred<TopicMetricsSession> = withContext(Dispatchers.IO) {
        async {
            try {
                pollAndCountEvents(eventType)
            } catch (e: Exception) {
                throw CountException("Klarte ikke å telle antall ${eventType.eventType}-eventer", e)
            }
        }

    }

    private fun pollAndCountEvents(eventType: EventType): TopicMetricsSession {

        val session = previousSession?.let { TopicMetricsSession(it) } ?: TopicMetricsSession(eventType)
        var records = kafkaConsumer.poll(Duration.of(5000, ChronoUnit.MILLIS))
        countBatch(records, session)

        while (records.foundRecords()) {
            records = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS))
            countBatch(records, session)
        }

        if (deltaCountingEnabled) {
            previousSession = session
        } else {
            kafkaConsumer.resetTheGroupIdsOffsetToZero()
        }

        session.calculateProcessingTime()

        return session
    }

    companion object {
        fun countBatch(records: ConsumerRecords<Nokkel, GenericRecord>, metricsSession: TopicMetricsSession) {
            records.forEach { record ->
                val event = UniqueKafkaEventIdentifierTransformer.toInternal(record)
                metricsSession.countEvent(event)
            }
        }
    }
}