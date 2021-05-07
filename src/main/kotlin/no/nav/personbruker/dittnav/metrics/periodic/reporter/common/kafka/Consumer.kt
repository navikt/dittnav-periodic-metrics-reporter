package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthCheck
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthStatus
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.Status
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

class Consumer<T>(
        val topic: String,
        val kafkaConsumer: KafkaConsumer<Nokkel, T>,
        val job: Job = Job(),
) : CoroutineScope, HealthCheck {

    private val log: Logger = LoggerFactory.getLogger(Consumer::class.java)
    private var numberOfFailedCounts = 0

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    suspend fun stop() {
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

    fun startSubscription() {
        log.info("Starter en subscription på topic: $topic.")
        kafkaConsumer.subscribe(listOf(topic))
    }

    fun getNumberOfFailedCounts(): Int {
        return numberOfFailedCounts
    }

    fun countNumberOfFailedCounts() {
        numberOfFailedCounts++
    }

    fun resetNumberOfFailedCounts() {
        numberOfFailedCounts = 0
    }
}
