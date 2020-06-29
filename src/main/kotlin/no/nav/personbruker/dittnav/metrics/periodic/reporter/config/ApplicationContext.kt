package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.buildDbEventCountingMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.buildTopicMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.CacheEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.MetricsRepository
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.KafkaEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.KafkaTopicEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.closeConsumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.createCountConsumer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter.PeriodicMetricsSubmitter
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

class ApplicationContext {

    private val log = LoggerFactory.getLogger(ApplicationContext::class.java)

    val environment = Environment()
    val database: Database = PostgresDatabase(environment)

    val healthService = HealthService(this)

    val kafkaTopicEventCounterService = KafkaTopicEventCounterService(environment)

    val metricsRepository = MetricsRepository(database)
    val cacheEventCounterService = CacheEventCounterService(environment, metricsRepository)
    val dbEventCountingMetricsProbe = buildDbEventCountingMetricsProbe(environment, database)
    val dbEventCounterService = DbEventCounterService(dbEventCountingMetricsProbe, metricsRepository)

    val topicMetricsProbe = buildTopicMetricsProbe(environment, database)

    val beskjedCountConsumer = createCountConsumer<GenericRecord>(EventType.BESKJED, Kafka.beskjedTopicName, environment)
    val innboksCountConsumer = createCountConsumer<GenericRecord>(EventType.INNBOKS, Kafka.innboksTopicName, environment)
    val oppgaveCountConsumer = createCountConsumer<GenericRecord>(EventType.OPPGAVE, Kafka.oppgaveTopicName, environment)
    val doneCountConsumer = createCountConsumer<GenericRecord>(EventType.DONE, Kafka.doneTopicName, environment)
    val topicEventCounterService = TopicEventCounterService(
            topicMetricsProbe = topicMetricsProbe,
            beskjedCountConsumer = beskjedCountConsumer,
            innboksCountConsumer = innboksCountConsumer,
            oppgaveCountConsumer = oppgaveCountConsumer,
            doneCountConsumer = doneCountConsumer
    )
    val kafkaEventCounterService = KafkaEventCounterService(
            beskjedCountConsumer = beskjedCountConsumer,
            innboksCountConsumer = innboksCountConsumer,
            oppgaveCountConsumer = oppgaveCountConsumer,
            doneCountConsumer = doneCountConsumer
    )

    var periodicMetricsSubmitter = initializePeriodicMetricsSubmitter()

    fun reinitializePeriodicMetricsSubmitter() {
        if (periodicMetricsSubmitter.isCompleted()) {
            periodicMetricsSubmitter = initializePeriodicMetricsSubmitter()
            log.info("periodicMetricsSubmitter har blitt reinstansiert.")
        } else {
            log.warn("periodicMetricsSubmitter kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }

    private fun initializePeriodicMetricsSubmitter(): PeriodicMetricsSubmitter =
            PeriodicMetricsSubmitter(dbEventCounterService, topicEventCounterService)

    fun closeAllConsumers() {
        closeConsumer(beskjedCountConsumer)
        closeConsumer(innboksCountConsumer)
        closeConsumer(oppgaveCountConsumer)
        closeConsumer(doneCountConsumer)
    }

}
