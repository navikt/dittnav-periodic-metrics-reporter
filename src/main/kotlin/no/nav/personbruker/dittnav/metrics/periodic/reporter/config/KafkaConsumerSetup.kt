package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.brukernotifikasjon.schemas.*
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object KafkaConsumerSetup {

    private val log: Logger = LoggerFactory.getLogger(KafkaConsumerSetup::class.java)

    fun <T> setupCountConsumer(kafkaProps: Properties, topic: String): Consumer<T> {
        val kafkaConsumer = KafkaConsumer<Nokkel, T>(kafkaProps)
        return Consumer(topic, kafkaConsumer)
    }

    fun startSubscriptionOnAllKafkaConsumers(appContext: ApplicationContext) {
        appContext.beskjedCountConsumerOnPrem.startSubscription()
        appContext.oppgaveCountConsumerOnPrem.startSubscription()
        appContext.doneCountConsumerOnPrem.startSubscription()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumerOnPrem.startSubscription()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte innboksconsumer.")
        }
    }

    suspend fun stopAllKafkaConsumers(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne...")
        appContext.beskjedCountConsumerOnPrem.stop()
        appContext.oppgaveCountConsumerOnPrem.stop()
        appContext.doneCountConsumerOnPrem.stop()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumerOnPrem.stop()
        }
        log.info("...ferdig med å stoppe kafka-pollerne.")
    }

    suspend fun restartConsumers(appContext: ApplicationContext) {
        stopAllKafkaConsumers(appContext)
        appContext.reinitializeConsumersOnPrem()
        appContext.reinitializeConsumersGCP()
        startSubscriptionOnAllKafkaConsumers(appContext)
    }

}
