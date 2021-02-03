package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.brukernotifikasjon.schemas.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object KafkaConsumerSetup {

    private val log: Logger = LoggerFactory.getLogger(KafkaConsumerSetup::class.java)

/*
    fun <T> createCountConsumer(eventType: EventType, topic: String, environment: Environment, enableSecurity: Boolean = ConfigUtil.isCurrentlyRunningOnNais()): KafkaConsumer<Nokkel, T> {
        val kafkaProps = Kafka.counterConsumerProps(environment, eventType, enableSecurity)
        val consumer = KafkaConsumer<Nokkel, T>(kafkaProps)
        consumer.subscribe(listOf(topic))
        return consumer
    }
*/
    fun <T> setupCountConsumer(kafkaProps: Properties, topic: String): Consumer<T> {
        val kafkaConsumer = KafkaConsumer<Nokkel, T>(kafkaProps)
        return Consumer(topic, kafkaConsumer)
    }

    fun startAllKafkaPollers(appContext: ApplicationContext) {
        appContext.beskjedCountConsumer.startPolling()
        appContext.oppgaveCountConsumer.startPolling()
        appContext.doneCountConsumer.startPolling()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumer.startPolling()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte innboksconsumer.")
        }
    }

    suspend fun stopAllKafkaConsumers(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne...")
        appContext.beskjedCountConsumer.stopPolling()
        appContext.oppgaveCountConsumer.stopPolling()
        appContext.doneCountConsumer.stopPolling()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumer.stopPolling()
        }
        log.info("...ferdig med å stoppe kafka-pollerne.")
    }

    suspend fun restartPolling(appContext: ApplicationContext) {
        stopAllKafkaConsumers(appContext)
        appContext.reinitializeConsumers()
        startAllKafkaPollers(appContext)
    }

    fun setupConsumerForTheBeskjedTopic(kafkaProps: Properties): Consumer<Beskjed> {
        val kafkaConsumer = KafkaConsumer<Nokkel, Beskjed>(kafkaProps)
        return Consumer(Kafka.beskjedTopicName, kafkaConsumer)
    }

    fun setupConsumerForTheOppgaveTopic(kafkaProps: Properties): Consumer<Oppgave> {
        val kafkaConsumer = KafkaConsumer<Nokkel, Oppgave>(kafkaProps)
        return Consumer(Kafka.oppgaveTopicName, kafkaConsumer)
    }

    fun setupConsumerForTheInnboksTopic(kafkaProps: Properties): Consumer<Innboks> {
        val kafkaConsumer = KafkaConsumer<Nokkel, Innboks>(kafkaProps)
        return Consumer(Kafka.innboksTopicName, kafkaConsumer)
    }

    fun setupConsumerForTheDoneTopic(kafkaProps: Properties): Consumer<Done> {
        val kafkaConsumer = KafkaConsumer<Nokkel, Done>(kafkaProps)
        return Consumer(Kafka.doneTopicName, kafkaConsumer)
    }

}
