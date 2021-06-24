package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.withContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.exceptions.CountException
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.foundRecords
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.resetTheGroupIdsOffsetToZero
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

class TopicEventTypeCounter<K>(
        val consumer: Consumer<K, GenericRecord>,
        val topicActivityService: TopicActivityService,
        val eventType: EventType,
        val deltaCountingEnabled: Boolean
) {

    private val log = LoggerFactory.getLogger(TopicEventTypeCounter::class.java)

    private var previousSession: TopicMetricsSession? = null

    private val timeoutConfig = TimeoutConfig(
            initialTimeout = Duration.ofMillis(9000),
            regularTimeut = Duration.ofMillis(1000),
            maxTotalTimeout = Duration.ofMinutes(3)
    )

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

        val startTime = Instant.now()

        val session = previousSession?.let { TopicMetricsSession(it) } ?: TopicMetricsSession(eventType)

        var records = consumer.kafkaConsumer.poll(timeoutConfig.pollingTimeout)

        if (records.foundRecords()) {
            topicActivityService.reportEventsFound()

            countBatch(records, session)

            while (records.foundRecords() && !maxTimeoutExceeded(startTime, timeoutConfig)) {

                records = consumer.kafkaConsumer.poll(timeoutConfig.pollingTimeout)
                countBatch(records, session)
            }

            if (deltaCountingEnabled) {
                previousSession = session
            } else {
                consumer.kafkaConsumer.resetTheGroupIdsOffsetToZero()
            }

        } else {
            topicActivityService.reportNoEventsFound()
        }

        session.calculateProcessingTime()

        return session
    }

    companion object {
        fun <K> countBatch(records: ConsumerRecords<K, GenericRecord>, metricsSession: TopicMetricsSession) {
            records.forEach { record ->
                val event = UniqueKafkaEventIdentifierTransformer.toInternal<K>(record)
                metricsSession.countEvent(event)
            }
        }
    }

    private fun maxTimeoutExceeded(start: Instant, config: TimeoutConfig): Boolean {
        return Instant.now() > start.plus(config.maxTotalTimeout)
    }

    fun getPreviousSession(): TopicMetricsSession? {
        return previousSession
    }

    private data class TimeoutConfig(private val initialTimeout: Duration,
                                     private val regularTimeut: Duration,
                                     val maxTotalTimeout: Duration) {

        private var isFirstInvocation = true

        val pollingTimeout: Duration get() {
                return if (isFirstInvocation) {
                    isFirstInvocation = false
                    initialTimeout
                } else {
                    regularTimeut
                }
            }

        init {
            require(initialTimeout < maxTotalTimeout) { "maxTotalTimeout må være høyere enn initialTimeout." }
            require(regularTimeut.toMillis() > 0) { "regularTimeout kan ikke være mindre enn 1 millisekund." }
        }

    }

}
