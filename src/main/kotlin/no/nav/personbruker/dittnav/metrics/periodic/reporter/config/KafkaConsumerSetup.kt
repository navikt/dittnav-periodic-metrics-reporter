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
        appContext.beskjedCountConsumer.startSubscription()
        appContext.oppgaveCountConsumer.startSubscription()
        appContext.doneCountConsumer.startSubscription()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumer.startSubscription()
            appContext.statusoppdateringCountConsumer.startSubscription()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte innboks- og statusoppdateringconsumer.")
        }
    }

    suspend fun stopAllKafkaConsumers(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne...")
        appContext.beskjedCountConsumer.stop()
        appContext.oppgaveCountConsumer.stop()
        appContext.doneCountConsumer.stop()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumer.stop()
            appContext.statusoppdateringCountConsumer.stop()
        }
        log.info("...ferdig med å stoppe kafka-pollerne.")
    }

    suspend fun restartConsumers(appContext: ApplicationContext) {
        stopAllKafkaConsumers(appContext)
        appContext.reinitializeConsumers()
        startSubscriptionOnAllKafkaConsumers(appContext)
    }

}
