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
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterAivenService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.TopicEventCounterOnPremService
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

    val dbEventCountingMetricsProbe = DbCountingMetricsProbe()
    val metricsReporter = resolveMetricsReporter(environment)

    val databaseOnPrem: Database = PostgresDatabase(environment)
    val metricsRepositoryOnPrem = MetricsRepository(databaseOnPrem)
    val dbEventCounterServiceOnPrem = DbEventCounterService(dbEventCountingMetricsProbe, metricsRepositoryOnPrem)

    val nameResolver = ProducerNameResolver(databaseOnPrem)
    val nameScrubber = ProducerNameScrubber(nameResolver)
    val healthService = HealthService(this)

    val dbMetricsReporter = DbMetricsReporter(metricsReporter, nameScrubber)
    val kafkaMetricsReporter = TopicMetricsReporter(metricsReporter, nameScrubber)

    val beskjedKafkaPropsOnPrem = Kafka.counterConsumerPropsOnPrem(environment, EventType.BESKJED)
    val beskjedKafkaPropsAiven = Kafka.counterConsumerPropsAiven(environment, EventType.BESKJED)
    var beskjedCountConsumerOnPrem = initializeCountConsumerOnPrem(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameOnPrem)
    var beskjedCountConsumerAiven = initializeCountConsumerAiven(beskjedKafkaPropsAiven, Kafka.beskjedTopicNameAiven)
    val beskjedCounterOnPrem = TopicEventTypeCounter(beskjedCountConsumerOnPrem, EventType.BESKJED, environment.deltaCountingEnabled)
    val beskjedCounterAiven = TopicEventTypeCounter(beskjedCountConsumerAiven, EventType.BESKJED_INTERN, environment.deltaCountingEnabled)

    val oppgaveKafkaPropsOnPrem = Kafka.counterConsumerPropsOnPrem(environment, EventType.OPPGAVE)
    val oppgaveKafkaPropsAiven = Kafka.counterConsumerPropsAiven(environment, EventType.OPPGAVE)
    var oppgaveCountConsumerOnPrem = initializeCountConsumerOnPrem(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameOnPrem)
    var oppgaveCountConsumerAiven = initializeCountConsumerAiven(oppgaveKafkaPropsAiven, Kafka.oppgaveTopicNameAiven)
    val oppgaveCounterOnPrem = TopicEventTypeCounter(oppgaveCountConsumerOnPrem, EventType.OPPGAVE, environment.deltaCountingEnabled)
    val oppgaveCounterAiven = TopicEventTypeCounter(oppgaveCountConsumerAiven, EventType.OPPGAVE_INTERN, environment.deltaCountingEnabled)

    val innboksKafkaPropsOnPrem = Kafka.counterConsumerPropsOnPrem(environment, EventType.INNBOKS)
    val innboksKafkaPropsAiven = Kafka.counterConsumerPropsAiven(environment, EventType.INNBOKS)
    var innboksCountConsumerOnPrem = initializeCountConsumerOnPrem(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameOnPrem)
    var innboksCountConsumerAiven = initializeCountConsumerAiven(innboksKafkaPropsAiven, Kafka.innboksTopicNameAiven)
    val innboksCounterOnPrem = TopicEventTypeCounter(innboksCountConsumerOnPrem, EventType.INNBOKS, environment.deltaCountingEnabled)
    val innboksCounterAiven = TopicEventTypeCounter(innboksCountConsumerAiven, EventType.INNBOKS_INTERN, environment.deltaCountingEnabled)

    val statusoppdateringKafkaPropsOnPrem = Kafka.counterConsumerPropsOnPrem(environment, EventType.STATUSOPPDATERING)
    val statusoppdateringKafkaPropsAiven = Kafka.counterConsumerPropsOnPrem(environment, EventType.STATUSOPPDATERING)
    var statusoppdateringCountConsumerOnPrem = initializeCountConsumerOnPrem(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameOnPrem)
    var statusoppdateringCountConsumerAiven = initializeCountConsumerAiven(statusoppdateringKafkaPropsAiven, Kafka.statusoppdateringTopicNameAiven)
    val statusoppdateringCounterOnPrem = TopicEventTypeCounter(statusoppdateringCountConsumerOnPrem, EventType.STATUSOPPDATERING, environment.deltaCountingEnabled)
    val statusoppdateringCounterAiven = TopicEventTypeCounter(statusoppdateringCountConsumerAiven, EventType.STATUSOPPDATERING_INTERN, environment.deltaCountingEnabled)

    val doneKafkaPropsOnPrem = Kafka.counterConsumerPropsOnPrem(environment, EventType.DONE)
    val doneKafkaPropsAiven = Kafka.counterConsumerPropsAiven(environment, EventType.DONE)
    var doneCountConsumerOnPrem = initializeCountConsumerOnPrem(doneKafkaPropsOnPrem, Kafka.doneTopicNameOnPrem)
    var doneCountConsumerAiven = initializeCountConsumerAiven(doneKafkaPropsAiven, Kafka.doneTopicNameAiven)
    val doneCounterOnPrem = TopicEventTypeCounter(doneCountConsumerOnPrem, EventType.DONE, environment.deltaCountingEnabled)
    val doneCounterAiven = TopicEventTypeCounter(doneCountConsumerAiven, EventType.DONE_INTERN, environment.deltaCountingEnabled)

    val topicEventCounterServiceOnPrem = TopicEventCounterOnPremService(
        beskjedCounter = beskjedCounterOnPrem,
        innboksCounter = innboksCounterOnPrem,
        oppgaveCounter = oppgaveCounterOnPrem,
        statusoppdateringCounter = statusoppdateringCounterOnPrem,
        doneCounter = doneCounterOnPrem
    )

    val topicEventCounterServiceAiven = TopicEventCounterAivenService(
        beskjedCounter = beskjedCounterAiven,
        innboksCounter = innboksCounterAiven,
        oppgaveCounter = oppgaveCounterAiven,
        statusoppdateringCounter = statusoppdateringCounterAiven,
        doneCounter = doneCounterAiven
    )

    val metricsSubmitterService = MetricsSubmitterService(
        dbEventCounterServiceOnPrem = dbEventCounterServiceOnPrem,
        topicEventCounterServiceOnPrem = topicEventCounterServiceOnPrem,
        topicEventCounterServiceAiven = topicEventCounterServiceAiven,
        dbMetricsReporter = dbMetricsReporter,
        kafkaMetricsReporter = kafkaMetricsReporter
    )

    var periodicMetricsSubmitter = initializePeriodicMetricsSubmitter()
    var periodicConsumerCheck = initializePeriodicConsumerCheck()

    private fun initializePeriodicConsumerCheck() =
            PeriodicConsumerCheck(this)

    private fun initializeCountConsumerOnPrem(kafkaProps: Properties, topic: String) =
            KafkaConsumerSetup.setupCountConsumerOnPrem<GenericRecord>(kafkaProps, topic)

    private fun initializeCountConsumerAiven(kafkaProps: Properties, topic: String) =
            KafkaConsumerSetup.setupCountConsumerAiven<GenericRecord>(kafkaProps, topic)

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

    fun reinitializeConsumersOnPrem() {
        if (beskjedCountConsumerOnPrem.isCompleted()) {
            beskjedCountConsumerOnPrem = initializeCountConsumerOnPrem(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameOnPrem)
            log.info("beskjedCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountConsumerOnPrem.isCompleted()) {
            oppgaveCountConsumerOnPrem = initializeCountConsumerOnPrem(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameOnPrem)
            log.info("oppgaveCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountConsumerOnPrem.isCompleted()) {
            innboksCountConsumerOnPrem = initializeCountConsumerOnPrem(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameOnPrem)
            log.info("innboksCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringCountConsumerOnPrem.isCompleted()) {
            statusoppdateringCountConsumerOnPrem = initializeCountConsumerOnPrem(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameOnPrem)
            log.info("statusoppdateringCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountConsumerOnPrem.isCompleted()) {
            doneCountConsumerOnPrem = initializeCountConsumerOnPrem(doneKafkaPropsOnPrem, Kafka.doneTopicNameOnPrem)
            log.info("doneConsumer har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }

    fun reinitializeConsumersAiven() {
        if (beskjedCountConsumerAiven.isCompleted()) {
            beskjedCountConsumerAiven = initializeCountConsumerAiven(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameAiven)
            log.info("beskjedCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountConsumerAiven.isCompleted()) {
            oppgaveCountConsumerAiven = initializeCountConsumerAiven(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameAiven)
            log.info("oppgaveCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountConsumerAiven.isCompleted()) {
            innboksCountConsumerAiven = initializeCountConsumerAiven(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameAiven)
            log.info("innboksCountConsumer på Aiven blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringCountConsumerAiven.isCompleted()) {
            statusoppdateringCountConsumerAiven = initializeCountConsumerAiven(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameAiven)
            log.info("statusoppdateringCountConsumer på Aiven blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountConsumerAiven.isCompleted()) {
            doneCountConsumerAiven = initializeCountConsumerAiven(doneKafkaPropsOnPrem, Kafka.doneTopicNameAiven)
            log.info("doneConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }
}

