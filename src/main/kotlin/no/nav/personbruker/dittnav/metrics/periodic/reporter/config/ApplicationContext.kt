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

    val dbEventCountingMetricsProbe = DbCountingMetricsProbe()
    val metricsReporter = resolveMetricsReporter(environment)

    val databaseOnPrem: Database = PostgresDatabase(environment)
    val metricsRepositoryOnPrem = MetricsRepository(databaseOnPrem)
    val dbEventCounterServiceOnPrem = DbEventCounterService(dbEventCountingMetricsProbe, metricsRepositoryOnPrem)

    val databaseGCP: Database = PostgresDatabase(environment)
    val metricsRepositoryGCP = MetricsRepository(databaseGCP)
    val dbEventCounterServiceGCP = DbEventCounterService(dbEventCountingMetricsProbe, metricsRepositoryGCP)

    val nameResolver = ProducerNameResolver(databaseOnPrem)
    val nameScrubber = ProducerNameScrubber(nameResolver)
    val healthService = HealthService(this)

    val dbMetricsReporter = DbMetricsReporter(metricsReporter, nameScrubber)
    val kafkaMetricsReporter = TopicMetricsReporter(metricsReporter, nameScrubber)

    val beskjedKafkaProps = Kafka.counterConsumerPropsOnPrem(environment, EventType.BESKJED)
    val oppgaveKafkaProps = Kafka.counterConsumerPropsOnPrem(environment, EventType.OPPGAVE)
    val innboksKafkaProps = Kafka.counterConsumerPropsOnPrem(environment, EventType.INNBOKS)
    val statusoppdateringKafkaProps = Kafka.counterConsumerPropsOnPrem(environment, EventType.STATUSOPPDATERING)
    val doneKafkaProps = Kafka.counterConsumerPropsOnPrem(environment, EventType.DONE)

    var beskjedCountConsumerOnPrem = initializeCountConsumer(beskjedKafkaProps, Kafka.beskjedTopicNameOnPrem)
    var innboksCountConsumerOnPrem = initializeCountConsumer(innboksKafkaProps, Kafka.innboksTopicNameOnPrem)
    var oppgaveCountConsumerOnPrem = initializeCountConsumer(oppgaveKafkaProps, Kafka.oppgaveTopicNameOnPrem)
    var statusoppdateringCountConsumerOnPrem = initializeCountConsumer(statusoppdateringKafkaProps, Kafka.statusoppdateringTopicNameOnPrem)
    var doneCountConsumerOnPrem = initializeCountConsumer(doneKafkaProps, Kafka.doneTopicNameOnPrem)

    var beskjedCountConsumerAiven = initializeCountConsumer(beskjedKafkaProps, Kafka.beskjedTopicNameAiven)
    var innboksCountConsumerGCP = initializeCountConsumer(innboksKafkaProps, Kafka.innboksTopicNameAiven)
    var oppgaveCountConsumerAiven = initializeCountConsumer(oppgaveKafkaProps, Kafka.oppgaveTopicNameAiven)
    var statusoppdateringCountConsumerGCP = initializeCountConsumer(statusoppdateringKafkaProps, Kafka.statusoppdateringTopicNameAiven)
    var doneCountConsumerGCP = initializeCountConsumer(doneKafkaProps, Kafka.doneTopicNameAiven)

    val beskjedCounterOnPrem = TopicEventTypeCounter(beskjedCountConsumerOnPrem, EventType.BESKJED, environment.deltaCountingEnabled)
    val innboksCounterOnPrem = TopicEventTypeCounter(innboksCountConsumerOnPrem, EventType.INNBOKS, environment.deltaCountingEnabled)
    val oppgaveCounterOnPrem = TopicEventTypeCounter(oppgaveCountConsumerOnPrem, EventType.OPPGAVE, environment.deltaCountingEnabled)
    val statusoppdateringCounterOnPrem = TopicEventTypeCounter(statusoppdateringCountConsumerOnPrem, EventType.STATUSOPPDATERING, environment.deltaCountingEnabled)
    val doneCounterOnPrem = TopicEventTypeCounter(doneCountConsumerOnPrem, EventType.DONE, environment.deltaCountingEnabled)

    val beskjedCounterGCP = TopicEventTypeCounter(beskjedCountConsumerAiven, EventType.BESKJED, environment.deltaCountingEnabled)
    val innboksCounterGCP = TopicEventTypeCounter(innboksCountConsumerGCP, EventType.INNBOKS, environment.deltaCountingEnabled)
    val oppgaveCounterGCP = TopicEventTypeCounter(oppgaveCountConsumerAiven, EventType.OPPGAVE, environment.deltaCountingEnabled)
    val statusoppdateringCounterGCP = TopicEventTypeCounter(statusoppdateringCountConsumerGCP, EventType.STATUSOPPDATERING, environment.deltaCountingEnabled)
    val doneCounterGCP = TopicEventTypeCounter(doneCountConsumerGCP, EventType.DONE, environment.deltaCountingEnabled)

    val topicEventCounterServiceOnPrem = TopicEventCounterService(
        beskjedCounter = beskjedCounterOnPrem,
        innboksCounter = innboksCounterOnPrem,
        oppgaveCounter = oppgaveCounterOnPrem,
        statusoppdateringCounter = statusoppdateringCounterOnPrem,
        doneCounter = doneCounterOnPrem
    )

    val topicEventCounterServiceGCP = TopicEventCounterService(
        beskjedCounter = beskjedCounterGCP,
        innboksCounter = innboksCounterGCP,
        oppgaveCounter = oppgaveCounterGCP,
        statusoppdateringCounter = statusoppdateringCounterGCP,
        doneCounter = doneCounterGCP
    )

    val metricsSubmitterService = MetricsSubmitterService(
        dbEventCounterServiceOnPrem = dbEventCounterServiceOnPrem,
        dbEventCounterServiceGCP = dbEventCounterServiceGCP,
        topicEventCounterServiceOnPrem = topicEventCounterServiceOnPrem,
        topicEventCounterServiceGCP = topicEventCounterServiceGCP,
        dbMetricsReporter = dbMetricsReporter,
        kafkaMetricsReporter = kafkaMetricsReporter
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

    fun reinitializeConsumersOnPrem() {
        if (beskjedCountConsumerOnPrem.isCompleted()) {
            beskjedCountConsumerOnPrem = initializeCountConsumer(beskjedKafkaProps, Kafka.beskjedTopicNameOnPrem)
            log.info("beskjedCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountConsumerOnPrem.isCompleted()) {
            oppgaveCountConsumerOnPrem = initializeCountConsumer(oppgaveKafkaProps, Kafka.oppgaveTopicNameOnPrem)
            log.info("oppgaveCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountConsumerOnPrem.isCompleted()) {
            innboksCountConsumerOnPrem = initializeCountConsumer(innboksKafkaProps, Kafka.innboksTopicNameOnPrem)
            log.info("innboksCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringCountConsumerOnPrem.isCompleted()) {
            statusoppdateringCountConsumerOnPrem = initializeCountConsumer(statusoppdateringKafkaProps, Kafka.statusoppdateringTopicNameOnPrem)
            log.info("statusoppdateringCountConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountConsumerOnPrem.isCompleted()) {
            doneCountConsumerOnPrem = initializeCountConsumer(doneKafkaProps, Kafka.doneTopicNameOnPrem)
            log.info("doneConsumer on-prem har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer on-prem kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }

    fun reinitializeConsumersAiven() {
        if (beskjedCountConsumerAiven.isCompleted()) {
            beskjedCountConsumerAiven = initializeCountConsumer(beskjedKafkaProps, Kafka.beskjedTopicNameAiven)
            log.info("beskjedCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountConsumerAiven.isCompleted()) {
            oppgaveCountConsumerAiven = initializeCountConsumer(oppgaveKafkaProps, Kafka.oppgaveTopicNameAiven)
            log.info("oppgaveCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountConsumerGCP.isCompleted()) {
            innboksCountConsumerGCP = initializeCountConsumer(innboksKafkaProps, Kafka.innboksTopicNameAiven)
            log.info("innboksCountConsumer på GCP blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer på GCP kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringCountConsumerGCP.isCompleted()) {
            statusoppdateringCountConsumerGCP = initializeCountConsumer(statusoppdateringKafkaProps, Kafka.statusoppdateringTopicNameAiven)
            log.info("statusoppdateringCountConsumer på GCP blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer på GCP kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountConsumerGCP.isCompleted()) {
            doneCountConsumerGCP = initializeCountConsumer(doneKafkaProps, Kafka.doneTopicNameAiven)
            log.info("doneConsumer på GCP har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer på GCP kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }
}

