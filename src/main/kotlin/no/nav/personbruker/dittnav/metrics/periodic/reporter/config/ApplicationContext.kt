package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.polling.PeriodicConsumerCheck
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.ActivityHealthDecider
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.ActivityHealthService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.ActivityMonitoringToggles
import no.nav.personbruker.dittnav.metrics.periodic.reporter.health.HealthService
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count.DbEventCounterGCPService
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
import no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.kafka.topic.activity.TopicActivityService
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
    val beskjedKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.BESKJED_INTERN)
    var beskjedCountOnPremConsumer = initializeCountConsumerOnPrem(beskjedKafkaPropsOnPrem, Kafka.beskjedTopicNameOnPrem)
    var beskjedCountAivenConsumer = initializeCountConsumerAiven(beskjedKafkaPropsAiven, Kafka.beskjedTopicNameAiven)
    val beskjedTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val beskjedAivenTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val beskjedCounterOnPrem = TopicEventTypeCounter(
        beskjedCountOnPremConsumer,
        beskjedTopicActivityService,
        EventType.BESKJED,
        environment.deltaCountingEnabled
    )
    val beskjedCounterAiven = TopicEventTypeCounter(
        beskjedCountAivenConsumer,
        beskjedAivenTopicActivityService,
        EventType.BESKJED_INTERN,
        environment.deltaCountingEnabled
    )

    val oppgaveKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.OPPGAVE)
    val oppgaveKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.OPPGAVE_INTERN)
    var oppgaveCountOnPremConsumer = initializeCountConsumerOnPrem(oppgaveKafkaPropsOnPrem, Kafka.oppgaveTopicNameOnPrem)
    var oppgaveCountAivenConsumer = initializeCountConsumerAiven(oppgaveKafkaPropsAiven, Kafka.oppgaveTopicNameAiven)
    val oppgaveTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val oppgaveAivenTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val oppgaveCounterOnPrem = TopicEventTypeCounter(
            oppgaveCountOnPremConsumer,
            oppgaveTopicActivityService,
            EventType.OPPGAVE,
            environment.deltaCountingEnabled
    )
    val oppgaveCounterAiven = TopicEventTypeCounter(
            oppgaveCountAivenConsumer,
            oppgaveAivenTopicActivityService,
            EventType.OPPGAVE_INTERN,
            environment.deltaCountingEnabled
    )

    val innboksKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.INNBOKS)
    val innboksKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.INNBOKS_INTERN)
    var innboksCountOnPremConsumer = initializeCountConsumerOnPrem(innboksKafkaPropsOnPrem, Kafka.innboksTopicNameOnPrem)
    var innboksCountAivenConsumer = initializeCountConsumerAiven(innboksKafkaPropsAiven, Kafka.innboksTopicNameAiven)
    val innboksTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val innboksAivenTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val innboksCounterOnPrem = TopicEventTypeCounter(
            innboksCountOnPremConsumer,
            innboksTopicActivityService,
            EventType.INNBOKS,
            environment.deltaCountingEnabled
    )
    val innboksCounterAiven = TopicEventTypeCounter(
            innboksCountAivenConsumer,
            innboksAivenTopicActivityService,
            EventType.INNBOKS_INTERN,
            environment.deltaCountingEnabled
    )

    val statusoppdateringKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.STATUSOPPDATERING)
    val statusoppdateringKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.STATUSOPPDATERING_INTERN)
    var statusoppdateringCountOnPremConsumer = initializeCountConsumerOnPrem(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameOnPrem)
    var statusoppdateringCountAivenConsumer = initializeCountConsumerAiven(statusoppdateringKafkaPropsAiven, Kafka.statusoppdateringTopicNameAiven)
    val statusoppdateringTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val statusoppdateringAivenTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val statusoppdateringCounterOnPrem = TopicEventTypeCounter(
            statusoppdateringCountOnPremConsumer,
            statusoppdateringTopicActivityService,
            EventType.STATUSOPPDATERING,
            environment.deltaCountingEnabled
    )
    val statusoppdateringCounterAiven = TopicEventTypeCounter(
            statusoppdateringCountAivenConsumer,
            statusoppdateringAivenTopicActivityService,
            EventType.STATUSOPPDATERING_INTERN,
            environment.deltaCountingEnabled
    )

    val doneKafkaPropsOnPrem = Kafka.counterConsumerOnPremProps(environment, EventType.DONE)
    val doneKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.DONE_INTERN)
    var doneCountOnPremConsumer = initializeCountConsumerOnPrem(doneKafkaPropsOnPrem, Kafka.doneTopicNameOnPrem)
    var doneCountAivenConsumer = initializeCountConsumerAiven(doneKafkaPropsAiven, Kafka.doneTopicNameAiven)
    val doneTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val doneAivenTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val doneCounterOnPrem = TopicEventTypeCounter(
            doneCountOnPremConsumer,
            doneTopicActivityService,
            EventType.DONE,
            environment.deltaCountingEnabled
    )
    val doneCounterAiven = TopicEventTypeCounter(
            doneCountAivenConsumer,
            doneAivenTopicActivityService,
            EventType.DONE_INTERN,
            environment.deltaCountingEnabled
    )

    val feilresponsKafkaPropsAiven = Kafka.counterConsumerAivenProps(environment, EventType.FEILRESPONS)
    var feilresponsCountAivenConsumer = initializeCountConsumerAiven(feilresponsKafkaPropsAiven, Kafka.feilresponsTopicNameAiven)
    val feilresponsAivenTopicActivityService = TopicActivityService(environment.activityHistoryLength)
    val feilresponsCounterAiven = TopicEventTypeCounter(
        feilresponsCountAivenConsumer,
        feilresponsAivenTopicActivityService,
        EventType.FEILRESPONS,
        environment.deltaCountingEnabled
    )

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
        doneCounter = doneCounterAiven,
        feilresponsCounter = feilresponsCounterAiven
    )

    val metricsSubmitterService = MetricsSubmitterService(
        dbEventCounterOnPremService = dbEventCounterOnPremService,
        dbEventCounterGCPService = dbEventCounterGCPService,
        topicEventCounterServiceOnPrem = topicEventCounterServiceOnPrem,
        topicEventCounterServiceAiven = topicEventCounterServiceAiven,
        dbMetricsReporter = dbMetricsReporter,
        kafkaMetricsReporter = kafkaMetricsReporter
    )

    val activityHealthServiceConfig = ActivityMonitoringToggles(
            monitorOnPremBeskjedActivity = environment.monitorOnPremBeskjedActivity,
            monitorOnPremOppgaveActivity = environment.monitorOnPremOppgaveActivity,
            monitorOnPremInnboksActivity = environment.monitorOnPremInnboksActivity,
            monitorOnPremDoneActivity = environment.monitorOnPremDoneActivity,
            monitorOnPremStatusoppdateringActivity = environment.monitorOnPremStatusoppdateringActivity
    )

    val activityHealthDecider = ActivityHealthDecider(
            lowActivityStreakThreshold = environment.lowActivityStreakThreshold,
            moderateActivityStreakThreshold = environment.moderateActivityStreakThreshold,
            highActivityStreakThreshold = environment.highActivityStreakThreshold
    )

    val activityHealthService = ActivityHealthService(
            beskjedTopicActivityService = beskjedTopicActivityService,
            oppgaveTopicActivityService = oppgaveTopicActivityService,
            innboksTopicActivityService = innboksTopicActivityService,
            doneTopicActivityService = doneTopicActivityService,
            statusoppdateringTopicActivityService = statusoppdateringTopicActivityService,
            activityHealthDecider = activityHealthDecider,
            monitoringToggles = activityHealthServiceConfig
    )

    var periodicMetricsSubmitter = initializePeriodicMetricsSubmitter()
    var periodicConsumerCheck = initializePeriodicConsumerCheck()

    private fun initializePeriodicConsumerCheck() =
            PeriodicConsumerCheck(this)

    private fun initializeCountConsumerOnPrem(kafkaProps: Properties, topic: String) =
            KafkaConsumerSetup.setupCountConsumer<Nokkel, GenericRecord>(kafkaProps, topic)

    private fun initializeCountConsumerAiven(kafkaProps: Properties, topic: String) =
            KafkaConsumerSetup.setupCountConsumer<NokkelIntern, GenericRecord>(kafkaProps, topic)

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

        if (statusoppdateringCountOnPremConsumer.isCompleted()) {
            statusoppdateringCountOnPremConsumer = initializeCountConsumerOnPrem(statusoppdateringKafkaPropsOnPrem, Kafka.statusoppdateringTopicNameOnPrem)
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
            beskjedCountAivenConsumer = initializeCountConsumerAiven(beskjedKafkaPropsAiven, Kafka.beskjedTopicNameAiven)
            log.info("beskjedCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("beskjedCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (oppgaveCountAivenConsumer.isCompleted()) {
            oppgaveCountAivenConsumer = initializeCountConsumerAiven(oppgaveKafkaPropsAiven, Kafka.oppgaveTopicNameAiven)
            log.info("oppgaveCountConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("oppgaveCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (innboksCountAivenConsumer.isCompleted()) {
            innboksCountAivenConsumer = initializeCountConsumerAiven(innboksKafkaPropsAiven, Kafka.innboksTopicNameAiven)
            log.info("innboksCountConsumer på Aiven blitt reinstansiert.")
        } else {
            log.warn("innboksCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (statusoppdateringCountAivenConsumer.isCompleted()) {
            statusoppdateringCountAivenConsumer = initializeCountConsumerAiven(statusoppdateringKafkaPropsAiven, Kafka.statusoppdateringTopicNameAiven)
            log.info("statusoppdateringCountConsumer på Aiven blitt reinstansiert.")
        } else {
            log.warn("statusoppdateringCountConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (doneCountAivenConsumer.isCompleted()) {
            doneCountAivenConsumer = initializeCountConsumerAiven(doneKafkaPropsAiven, Kafka.doneTopicNameAiven)
            log.info("doneConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("doneConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }

        if (feilresponsCountAivenConsumer.isCompleted()) {
            feilresponsCountAivenConsumer = initializeCountConsumerAiven(feilresponsKafkaPropsAiven, Kafka.feilresponsTopicNameAiven)
            log.info("feilresponsConsumer på Aiven har blitt reinstansiert.")
        } else {
            log.warn("feilresponsConsumer på Aiven kunne ikke bli reinstansiert fordi den fortsatt er aktiv.")
        }
    }
}

