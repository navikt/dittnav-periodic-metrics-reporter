package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka

import kotlinx.coroutines.*
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthCheck
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthStatus
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.Status
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.temporal.ChronoUnit
import kotlin.coroutines.CoroutineContext

class Consumer<T>(
        val topic: String,
        val kafkaConsumer: KafkaConsumer<Nokkel, T>,
        val job: Job = Job(),
        val maxPollTimeout: Long = 100L
) : CoroutineScope, HealthCheck {

    private val log: Logger = LoggerFactory.getLogger(Consumer::class.java)

    companion object {
        private const val ONE_MINUTE_IN_MS = 60000L
    }

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    suspend fun stopPolling() {
        job.cancelAndJoin()
    }

    fun isCompleted(): Boolean {
        return job.isCompleted
    }

    fun isStopped(): Boolean {
        return !job.isActive
    }

    override suspend fun status(): HealthStatus {
        val serviceName = topic + "consumer"
        return if (job.isActive) {
            HealthStatus(serviceName, Status.OK, "Consumer is running", includeInReadiness = false)
        } else {
            log.error("Selftest mot Kafka-consumere feilet, consumer kjører ikke.")
            HealthStatus(serviceName, Status.ERROR, "Consumer is not running", includeInReadiness = false)
        }
    }

    fun startPolling() {
        launch {
            log.info("Starter en coroutine for polling på topic-en $topic.")
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic))
            }
        }
    }
}
