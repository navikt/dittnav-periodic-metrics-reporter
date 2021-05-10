package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling.PeriodicConsumerCheck
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameResolver
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.MetricsRepository
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventTypeCounter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.resolveMetricsReporter
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter.MetricsSubmitterService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.submitter.PeriodicMetricsSubmitter
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import java.util.*

class ApplicationContext {

    private val log = LoggerFactory.getLogger(ApplicationContext::class.java)

    val environment = Environment()
    val database: Database = PostgresDatabase(environment)

    val healthService = HealthService(this)

    val nameResolver = ProducerNameResolver(database)
    val nameScrubber = ProducerNameScrubber(nameResolver)
    val metricsReporter = resolveMetricsReporter(environment)

    val metricsRepository = MetricsRepository(database)
    val dbEventCountingMetricsProbe = DbCountingMetricsProbe()
    val dbEventCounterService = DbEventCounterService(dbEventCountingMetricsProbe, metricsRepository)
    val dbMetricsReporter = DbMetricsReporter(metricsReporter, nameScrubber)

    val kafkaMetricsReporter = TopicMetricsReporter(metricsReporter, nameScrubber)

    val beskjedKafkaProps = Kafka.counterConsumerProps(environment, EventType.BESKJED)
    val oppgaveKafkaProps = Kafka.counterConsumerProps(environment, EventType.OPPGAVE)
    val innboksKafkaProps = Kafka.counterConsumerProps(environment, EventType.INNBOKS)
    val statusoppdateringKafkaProps = Kafka.counterConsumerProps(environment, EventType.STATUSOPPDATERING)
    val doneKafkaProps = Kafka.counterConsumerProps(environment, EventType.DONE)

    var beskjedCountConsumer = initializeCountConsumer(beskjedKafkaProps, Kafka.beskjedTopicName)
    var innboksCountConsumer = initializeCountConsumer(innboksKafkaProps, Kafka.innboksTopicName)
    var oppgaveCountConsumer = initializeCountConsumer(oppgaveKafkaProps, Kafka.oppgaveTopicName)
    var statusoppdateringCountConsumer = initializeCountConsumer(statusoppdateringKafkaProps, Kafka.statusoppdateringTopicName)
    var doneCountConsumer = initializeCountConsumer(doneKafkaProps, Kafka.doneTopicName)

    val beskjedCounter = TopicEventTypeCounter(beskjedCountConsumer, EventType.BESKJED, environment.deltaCountingEnabled)
    val innboksCounter = TopicEventTypeCounter(innboksCountConsumer, EventType.INNBOKS, environment.deltaCountingEnabled)
    val oppgaveCounter = TopicEventTypeCounter(oppgaveCountConsumer, EventType.OPPGAVE, environment.deltaCountingEnabled)
    val statusoppdateringCounter = TopicEventTypeCounter(statusoppdateringCountConsumer, EventType.STATUSOPPDATERING, environment.deltaCountingEnabled)
    val doneCounter = TopicEventTypeCounter(doneCountConsumer, EventType.DONE, environment.deltaCountingEnabled)

    val topicEventCounterService = TopicEventCounterService(
            beskjedCounter = beskjedCounter,
            innboksCounter = innboksCounter,
            oppgaveCounter = oppgaveCounter,
            statusoppdateringCounter = statusoppdateringCounter,
            doneCounter = doneCounter
    )

    val metricsSubmitterService = MetricsSubmitterService(
            dbEventCounterService,
            topicEventCounterService,
            dbMetricsReporter,
            kafkaMetricsReporter
    )

    var periodicMetricsSubmitter = initializePeriodicMetricsSubmitter()
    var periodicConsumerCheck = initializePeriodicConsumerCheck()

    private fun initializePeriodicConsumerCheck() =
            PeriodicConsumerCheck(this)

    private fun initializeCountConsumer(kafkaProps: Properties, topic: String) =
            KafkaConsumerSetup.setupCountConsumer<GenericRecord>(kafkaProps, topic)

    private fun initializePeriodicMetricsSubmitter(): PeriodicMetricsSubmitter =
            PeriodicMetricsSubmitter(metricsSubmitterService, environment.countingIntervalMinutes)

    fun reinitializePeriodicMetricsSubmitter() {
        if (periodicMetricsSubmitter.isCompleted()) {
            periodicMetricsSubmitter = initializePeriodicMetricsSubmitter()
            log.info("periodicMetricsSubmitter har blitt reinstansiert.")
        } else {
            log.warn("periodicMetricsSubmitter kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }

    fun reinitializePeriodicConsumerCheck() {
        if (periodicConsumerCheck.isCompleted()) {
            periodicConsumerCheck = initializePeriodicConsumerCheck()
            log.info("periodicConsumerCheck har blitt reinstansiert.")
        } else {
            log.warn("periodicConsumerCheck kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }

    fun reinitializeConsumers() {
        if (beskjedCountConsumer.isCompleted()) {
            beskjedCountConsumer = initializeCountConsumer(beskjedKafkaProps, Kafka.beskjedTopicName)
            log.info("beskjedCountConsumer har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountConsumer.isCompleted()) {
            oppgaveCountConsumer = initializeCountConsumer(oppgaveKafkaProps, Kafka.oppgaveTopicName)
            log.info("oppgaveCountConsumer har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountConsumer.isCompleted()) {
            innboksCountConsumer = initializeCountConsumer(innboksKafkaProps, Kafka.innboksTopicName)
            log.info("innboksCountConsumer har blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringCountConsumer.isCompleted()) {
            statusoppdateringCountConsumer = initializeCountConsumer(statusoppdateringKafkaProps, Kafka.statusoppdateringTopicName)
            log.info("statusoppdateringCountConsumer har blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountConsumer.isCompleted()) {
            doneCountConsumer = initializeCountConsumer(doneKafkaProps, Kafka.doneTopicName)
            log.info("doneConsumer har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }
}
