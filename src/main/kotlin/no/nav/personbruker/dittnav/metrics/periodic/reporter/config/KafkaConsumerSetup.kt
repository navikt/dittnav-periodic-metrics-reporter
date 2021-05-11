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

    fun startSubscriptionOnAllKafkaConsumersOnPrem(appContext: ApplicationContext) {
        appContext.beskjedCountConsumerOnPrem.startSubscription()
        appContext.oppgaveCountConsumerOnPrem.startSubscription()
        appContext.doneCountConsumerOnPrem.startSubscription()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumerOnPrem.startSubscription()
            appContext.statusoppdateringCountConsumerOnPrem.startSubscription()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte innboks- og statusoppdateringconsumer on prem.")
        }
    }

    fun startSubscriptionOnAllKafkaConsumersAiven(appContext: ApplicationContext) {
        if(isOtherEnvironmentThanProd()) {
            appContext.beskjedCountConsumerAiven.startSubscription()
            /** @TODO Foreløpig bare beskjed som konsumeres på Aiven
            appContext.oppgaveCountConsumerAiven.startSubscription()
            appContext.doneCountConsumerAiven.startSubscription()
            appContext.innboksCountConsumerOnPrem.startSubscription()
            appContext.statusoppdateringCountConsumerOnPrem.startSubscription()
             **/
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte consumere på Aiven.")
        }
    }

    suspend fun stopAllKafkaConsumersOnPrem(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne on prem...")
        appContext.beskjedCountConsumerOnPrem.stop()
        appContext.oppgaveCountConsumerOnPrem.stop()
        appContext.doneCountConsumerOnPrem.stop()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountConsumerOnPrem.stop()
            appContext.statusoppdateringCountConsumerOnPrem.stop()
        }
        log.info("...ferdig med å stoppe kafka-pollerne on prem.")
    }

    suspend fun stopAllKafkaConsumersAiven(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne på Aiven...")
        if(isOtherEnvironmentThanProd()) {
            appContext.beskjedCountConsumerAiven.stop()
            /** @TODO Foreløpig bare beskjed som konsumeres på Aiven
            appContext.oppgaveCountConsumerAiven.stop()
            appContext.doneCountConsumerAiven.stop()
            appContext.innboksCountConsumerAiven.stop()
            appContext.statusoppdateringCountConsumerAiven.stop()
            **/
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
