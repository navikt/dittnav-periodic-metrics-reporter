package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling

import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ApplicationContext
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.KafkaConsumerSetup
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthStatus
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.Status
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class PeriodicConsumerCheck(
        private val appContext: ApplicationContext,
        private val job: Job = Job()) : CoroutineScope {

    private val log: Logger = LoggerFactory.getLogger(PeriodicConsumerCheck::class.java)
    private val minutesToWait = Duration.ofMinutes(30)

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.Default + job

    fun start() {
        log.info("Periodisk sjekking at konsumerne kjører, første sjekk skjer om $minutesToWait minutter.")
        launch {
            while (job.isActive) {
                delay(minutesToWait)
                checkIfConsumersAreRunningAndRestartIfNot()
            }
        }
    }

    suspend fun checkIfConsumersAreRunningAndRestartIfNot() {
        val stoppedConsumers = getConsumersThatHaveStopped()
        if (stoppedConsumers.isNotEmpty()) {
            restartConsumers(stoppedConsumers)
        }
    }

    fun getConsumersThatHaveStopped(): MutableList<EventType> {
        val stoppedConsumers = mutableListOf<EventType>()

        if (appContext.beskjedCountConsumer.isStopped() || appContext.beskjedCountConsumer.getNumberOfFailedCounts() > appContext.environment.maxFailedCounts) {
            stoppedConsumers.add(EventType.BESKJED)
        }
        if (appContext.doneCountConsumer.isStopped() || appContext.doneCountConsumer.getNumberOfFailedCounts() > appContext.environment.maxFailedCounts) {
            stoppedConsumers.add(EventType.DONE)
        }
        if (appContext.oppgaveCountConsumer.isStopped() || appContext.oppgaveCountConsumer.getNumberOfFailedCounts() > appContext.environment.maxFailedCounts) {
            stoppedConsumers.add(EventType.OPPGAVE)
        }
        return stoppedConsumers
    }

    suspend fun restartConsumers(stoppedConsumers: MutableList<EventType>) {
        log.warn("Følgende konsumere hadde stoppet eller klarte ikke å telle eventer: ${stoppedConsumers}, de(n) vil bli restartet.")
        KafkaConsumerSetup.restartConsumers(appContext)
        log.info("$stoppedConsumers konsumern(e) har blitt restartet.")
    }

    suspend fun stop() {
        log.info("Stopper periodisk sjekking av at konsumerne kjører.")
        job.cancelAndJoin()
    }

    fun isCompleted(): Boolean {
        return job.isCompleted
    }

    fun status(): HealthStatus {
        return when (job.isActive) {
            true -> HealthStatus("PeriodicConsumerPollingCheck", Status.OK, "Checker is running", false)
            false -> HealthStatus("PeriodicConsumerPollingCheck", Status.ERROR, "Checker is not running", false)
        }
    }

}