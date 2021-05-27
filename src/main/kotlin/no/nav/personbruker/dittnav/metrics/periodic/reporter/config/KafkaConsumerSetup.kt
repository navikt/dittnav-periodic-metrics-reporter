package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object KafkaConsumerSetup {

    private val log: Logger = LoggerFactory.getLogger(KafkaConsumerSetup::class.java)

    fun <K, V> setupCountConsumer(kafkaProps: Properties, topic: String): Consumer<K, V> {
        val kafkaConsumer = KafkaConsumer<K, V>(kafkaProps)
        return Consumer(topic, kafkaConsumer)
    }

    fun startSubscriptionOnAllKafkaConsumersOnPrem(appContext: ApplicationContext) {
        appContext.beskjedCountOnPremConsumer.startSubscription()
        appContext.oppgaveCountOnPremConsumer.startSubscription()
        appContext.doneCountOnPremConsumer.startSubscription()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountOnPremConsumer.startSubscription()
            appContext.statusoppdateringCountOnPremConsumer.startSubscription()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte innboks- og statusoppdateringconsumer on prem.")
        }
    }

    fun startSubscriptionOnAllKafkaConsumersAiven(appContext: ApplicationContext) {
        if(isOtherEnvironmentThanProd()) {
            appContext.beskjedCountAivenConsumer.startSubscription()
            appContext.oppgaveCountAivenConsumer.startSubscription()
            appContext.doneCountAivenConsumer.startSubscription()
            appContext.feilresponsCountAivenConsumer.startSubscription()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte consumere på Aiven.")
        }
    }

    suspend fun stopAllKafkaConsumersOnPrem(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne on prem...")
        appContext.beskjedCountOnPremConsumer.stop()
        appContext.oppgaveCountOnPremConsumer.stop()
        appContext.doneCountOnPremConsumer.stop()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountOnPremConsumer.stop()
            appContext.statusoppdateringCountOnPremConsumer.stop()
        }
        log.info("...ferdig med å stoppe kafka-pollerne on prem.")
    }

    suspend fun stopAllKafkaConsumersAiven(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne på Aiven...")
        if(isOtherEnvironmentThanProd()) {
            appContext.beskjedCountAivenConsumer.stop()
            appContext.oppgaveCountAivenConsumer.stop()
            appContext.doneCountAivenConsumer.stop()
            appContext.feilresponsCountAivenConsumer.stop()
        }
        log.info("...ferdig med å stoppe kafka-pollerne på Aiven.")
    }

    suspend fun restartConsumersOnPrem(appContext: ApplicationContext) {
        stopAllKafkaConsumersOnPrem(appContext)
        appContext.reinitializeConsumersOnPrem()
        startSubscriptionOnAllKafkaConsumersOnPrem(appContext)
    }

    suspend fun restartConsumersAiven(appContext: ApplicationContext) {
        stopAllKafkaConsumersAiven(appContext)
        appContext.reinitializeConsumersAiven()
        startSubscriptionOnAllKafkaConsumersAiven(appContext)
    }
}
