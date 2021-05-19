package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.brukernotifikasjon.schemas.*
import no.nav.brukernotifikasjon.schemas.internal.NokkelIntern
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

object KafkaConsumerSetup {

    private val log: Logger = LoggerFactory.getLogger(KafkaConsumerSetup::class.java)

    fun <T> setupCountOnPremConsumer(kafkaProps: Properties, topic: String): Consumer<Nokkel, T> {
        val kafkaConsumer = KafkaConsumer<Nokkel, T>(kafkaProps)
        return Consumer(topic, kafkaConsumer)
    }

    fun <T> setupCountAivenConsumer(kafkaProps: Properties, topic: String): Consumer<NokkelIntern, T> {
        val kafkaConsumer = KafkaConsumer<NokkelIntern, T>(kafkaProps)
        return Consumer(topic, kafkaConsumer)
    }

    fun startSubscriptionOnAllKafkaConsumersOnPrem(appContext: ApplicationContext) {
        appContext.beskjedCountOnPremConsumer.startSubscription()
        appContext.oppgaveCountOnPremConsumer.startSubscription()
        appContext.doneCountOnPremConsumer.startSubscription()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountOnPremConsumer.startSubscription()
            appContext.statusoppdateringOnPremConsumer.startSubscription()
        } else {
            log.info("Er i produksjonsmiljø, unnlater å starte innboks- og statusoppdateringconsumer on prem.")
        }
    }

    fun startSubscriptionOnAllKafkaConsumersAiven(appContext: ApplicationContext) {
        if(isOtherEnvironmentThanProd()) {
            appContext.beskjedCountAivenConsumer.startSubscription()
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
        appContext.beskjedCountOnPremConsumer.stop()
        appContext.oppgaveCountOnPremConsumer.stop()
        appContext.doneCountOnPremConsumer.stop()
        if (isOtherEnvironmentThanProd()) {
            appContext.innboksCountOnPremConsumer.stop()
            appContext.statusoppdateringOnPremConsumer.stop()
        }
        log.info("...ferdig med å stoppe kafka-pollerne on prem.")
    }

    suspend fun stopAllKafkaConsumersAiven(appContext: ApplicationContext) {
        log.info("Begynner å stoppe kafka-pollerne på Aiven...")
        if(isOtherEnvironmentThanProd()) {
            appContext.beskjedCountAivenConsumer.stop()
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
