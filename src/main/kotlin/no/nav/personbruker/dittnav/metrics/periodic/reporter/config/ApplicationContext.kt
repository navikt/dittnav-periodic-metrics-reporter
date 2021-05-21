package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling.PeriodicConsumerCheck
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.DbEventCounterGCPService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameResolver
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.ProducerNameScrubber
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbCountingMetricsProbe
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterOnPremService
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
    val databaseGCP: Database = PostgresDatabase(environment)
    val metricsRepositoryOnPrem = MetricsRepository(databaseOnPrem)
    val metricsRepositoryGCP = MetricsRepository(databaseGCP)
    val dbEventCounterOnPremService = DbEventCounterOnPremService(dbEventCountingMetricsProbe, metricsRepositoryOnPrem)
    val dbEventCounterGCPService = DbEventCounterGCPService(dbEventCountingMetricsProbe, metricsRepositoryGCP)

    val nameResolver = ProducerNameResolver(databaseOnPrem)
    val nameScrubber = ProducerNameScrubber(nameResolver)
    val healthService = HealthService(this)

    val dbMetricsReporter = DbMetricsReporter(metricsReporter, nameScrubber)
    val kafkaMetricsReporter = TopicMetricsReporter(metricsReporter, nameScrubber)

    val beskjedKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.BESKJED)
    val beskjedKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.BESKJED)
    var beskjedCountOnPremConsumer = initializeCountConsumerOnPrem(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameOnPrem)
    var beskjedCountAivenConsumer = initializeCountConsumerAiven(beskjedKafkaPropsAiven, Kafka.beskjedTopicNameAiven)
    val beskjedCounterOnPrem = TopicEventTypeCounter(beskjedCountOnPremConsumer, EventType.BESKJED, environment.deltaCountingEnabled)
    val beskjedCounterAiven = TopicEventTypeCounter(beskjedCountAivenConsumer, EventType.BESKJED_INTERN, environment.deltaCountingEnabled)

    val oppgaveKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.OPPGAVE)
    val oppgaveKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.OPPGAVE)
    var oppgaveCountOnPremConsumer = initializeCountConsumerOnPrem(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameOnPrem)
    var oppgaveCountAivenConsumer = initializeCountConsumerAiven(oppgaveKafkaPropsAiven, Kafka.oppgaveTopicNameAiven)
    val oppgaveCounterOnPrem = TopicEventTypeCounter(oppgaveCountOnPremConsumer, EventType.OPPGAVE, environment.deltaCountingEnabled)
    val oppgaveCounterAiven = TopicEventTypeCounter(oppgaveCountAivenConsumer, EventType.OPPGAVE_INTERN, environment.deltaCountingEnabled)

    val innboksKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.INNBOKS)
    val innboksKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.INNBOKS)
    var innboksCountOnPremConsumer = initializeCountConsumerOnPrem(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameOnPrem)
    var innboksCountAivenConsumer = initializeCountConsumerAiven(innboksKafkaPropsAiven, Kafka.innboksTopicNameAiven)
    val innboksCounterOnPrem = TopicEventTypeCounter(innboksCountOnPremConsumer, EventType.INNBOKS, environment.deltaCountingEnabled)
    val innboksCounterAiven = TopicEventTypeCounter(innboksCountAivenConsumer, EventType.INNBOKS_INTERN, environment.deltaCountingEnabled)

    val statusoppdateringKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.STATUSOPPDATERING)
    val statusoppdateringKafkaPropsAiven = Kafka.counterConsumerOnPremProps(environment, EventType.STATUSOPPDATERING)
    var statusoppdateringOnPremConsumer = initializeCountConsumerOnPrem(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameOnPrem)
    var statusoppdateringAivenConsumer = initializeCountConsumerAiven(statusoppdateringKafkaPropsAiven, Kafka.statusoppdateringTopicNameAiven)
    val statusoppdateringCounterOnPrem = TopicEventTypeCounter(statusoppdateringOnPremConsumer, EventType.STATUSOPPDATERING, environment.deltaCountingEnabled)
    val statusoppdateringCounterAiven = TopicEventTypeCounter(statusoppdateringAivenConsumer, EventType.STATUSOPPDATERING_INTERN, environment.deltaCountingEnabled)

    val doneKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.DONE)
    val doneKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.DONE)
    var doneCountOnPremConsumer = initializeCountConsumerOnPrem(doneKafkaPropsOnPrem, Kafka.doneTopicNameOnPrem)
    var doneCountAivenConsumer = initializeCountConsumerAiven(doneKafkaPropsAiven, Kafka.doneTopicNameAiven)
    val doneCounterOnPrem = TopicEventTypeCounter(doneCountOnPremConsumer, EventType.DONE, environment.deltaCountingEnabled)
    val doneCounterAiven = TopicEventTypeCounter(doneCountAivenConsumer, EventType.DONE_INTERN, environment.deltaCountingEnabled)

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
        dbEventCounterOnPremService = dbEventCounterOnPremService,
        dbEventCounterGCPService = dbEventCounterGCPService,
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
            KafkaConsumerSetup.setupCountOnPremConsumer<GenericRecord>(kafkaProps, topic)

    private fun initializeCountConsumerAiven(kafkaProps: Properties, topic: String) =
            KafkaConsumerSetup.setupCountAivenConsumer<GenericRecord>(kafkaProps, topic)

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
        if (beskjedCountOnPremConsumer.isCompleted()) {
            beskjedCountOnPremConsumer = initializeCountConsumerOnPrem(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameOnPrem)
            log.info("beskjedCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountOnPremConsumer.isCompleted()) {
            oppgaveCountOnPremConsumer = initializeCountConsumerOnPrem(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameOnPrem)
            log.info("oppgaveCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountOnPremConsumer.isCompleted()) {
            innboksCountOnPremConsumer = initializeCountConsumerOnPrem(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameOnPrem)
            log.info("innboksCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringOnPremConsumer.isCompleted()) {
            statusoppdateringOnPremConsumer = initializeCountConsumerOnPrem(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameOnPrem)
            log.info("statusoppdateringCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountOnPremConsumer.isCompleted()) {
            doneCountOnPremConsumer = initializeCountConsumerOnPrem(doneKafkaPropsOnPrem, Kafka.doneTopicNameOnPrem)
            log.info("doneConsumer har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }

    fun reinitializeConsumersAiven() {
        if (beskjedCountAivenConsumer.isCompleted()) {
            beskjedCountAivenConsumer = initializeCountConsumerAiven(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameAiven)
            log.info("beskjedCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountAivenConsumer.isCompleted()) {
            oppgaveCountAivenConsumer = initializeCountConsumerAiven(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameAiven)
            log.info("oppgaveCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountAivenConsumer.isCompleted()) {
            innboksCountAivenConsumer = initializeCountConsumerAiven(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameAiven)
            log.info("innboksCountConsumer på Aiven blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringAivenConsumer.isCompleted()) {
            statusoppdateringAivenConsumer = initializeCountConsumerAiven(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameAiven)
            log.info("statusoppdateringCountConsumer på Aiven blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountAivenConsumer.isCompleted()) {
            doneCountAivenConsumer = initializeCountConsumerAiven(doneKafkaPropsOnPrem, Kafka.doneTopicNameAiven)
            log.info("doneConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }
}

