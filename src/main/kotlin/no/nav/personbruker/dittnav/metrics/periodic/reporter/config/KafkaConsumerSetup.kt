package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.brukernotifikasjon.schemas.Nokkel
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object KafkaConsumerSetup {

    private val log: Logger = LoggerFactory.getLogger(KafkaConsumerSetup::class.java)

    fun <T> createCountConsumer(eventType: EventType,
                                topic: String,
                                environment: Environment,
                                enableSecurity: Boolean = ConfigUtil.isCurrentlyRunningOnNais()): KafkaConsumer<Nokkel, T> {

        val kafkaProps = Kafka.counterConsumerProps(environment, eventType, enableSecurity)
        val consumer = KafkaConsumer<Nokkel, T>(kafkaProps)
        consumer.subscribe(listOf(topic))
        return consumer
    }

}
