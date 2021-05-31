package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.netty.util.NetUtil.getHostname
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.kafka.SwallowSerializationErrorsAvroDeserializer
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.ConfigUtil.isCurrentlyRunningOnNais
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.net.InetSocketAddress
import java.util.*

object Kafka {

    private val log: Logger = LoggerFactory.getLogger(Kafka::class.java)

    const val doneTopicNameOnPrem = "aapen-brukernotifikasjon-done-v1"
    const val beskjedTopicNameOnPrem = "aapen-brukernotifikasjon-nyBeskjed-v1"
    const val innboksTopicNameOnPrem = "aapen-brukernotifikasjon-nyInnboks-v1"
    const val oppgaveTopicNameOnPrem = "aapen-brukernotifikasjon-nyOppgave-v1"
    const val statusoppdateringTopicNameOnPrem = "aapen-brukernotifikasjon-nyStatusoppdatering-v1"

    const val doneTopicNameAiven = "min-side.privat-brukernotifikasjon-done-v1"
    const val beskjedTopicNameAiven = "min-side.privat-brukernotifikasjon-beskjed-v1"
    const val innboksTopicNameAiven = "min-side.privat-brukernotifikasjon-innboks-v1"
    const val oppgaveTopicNameAiven = "min-side.privat-brukernotifikasjon-oppgave-v1"
    const val statusoppdateringTopicNameAiven = "min-side.privat-brukernotifikasjon-statusoppdatering-v1"
    const val feilresponsTopicNameAiven = "min-side.aapen-brukernotifikasjon-feilrespons-v1"

    fun counterConsumerOnPremProps(env: Environment, eventTypeToConsume: EventType, enableSecurity: Boolean = isCurrentlyRunningOnNais()): Properties {
        return Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.bootstrapServers)
            put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, env.schemaRegistryUrl)
            commonProps(env, eventTypeToConsume)
            if (enableSecurity) {
                putAll(credentialPropsOnPrem(env))
            }
        }
    }

    fun counterConsumerAivenProps(env: Environment, eventTypeToConsume: EventType, enableSecurity: Boolean = isCurrentlyRunningOnNais()): Properties {
        return Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.aivenBrokers)
            put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, env.aivenSchemaRegistry)
            commonProps(env, eventTypeToConsume)
            if (enableSecurity) {
                putAll(credentialPropsAiven(env))
            }
        }
    }

    private fun credentialPropsOnPrem(env: Environment): Properties {
        return Properties().apply {
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            put(SaslConfigs.SASL_JAAS_CONFIG,
                """org.apache.kafka.common.security.plain.PlainLoginModule required username="${env.username}" password="${env.password}";""")
            System.getenv("NAV_TRUSTSTORE_PATH")?.let {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, File(it).absolutePath)
                put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, System.getenv("NAV_TRUSTSTORE_PASSWORD"))
                log.info("Configured ${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG} location")
            }
        }
    }

    private fun credentialPropsAiven(env: Environment): Properties {
        return Properties().apply {
            put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, "${env.aivenSchemaRegistryUser}:${env.aivenSchemaRegistryPassword}")
            put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO")
            put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
            put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
            put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.aivenTruststorePath)
            put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, env.aivenCredstorePassword)
            put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.aivenKeystorePath)
            put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.aivenCredstorePassword)
            put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, env.aivenCredstorePassword)
            put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        }
    }

    private fun Properties.commonProps(env: Environment, eventTypeToConsume: EventType) {
        val groupIdAndEventType = "${env.groupIdBase}_${eventTypeToConsume.eventType}"
        val sixMinutes = 6 * 60 * 1000
        put(ConsumerConfig.GROUP_ID_CONFIG, groupIdAndEventType)
        put(ConsumerConfig.CLIENT_ID_CONFIG, groupIdAndEventType + getHostname(InetSocketAddress(0)))
        put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, sixMinutes)
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, SwallowSerializationErrorsAvroDeserializer::class.java)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SwallowSerializationErrorsAvroDeserializer::class.java)
        put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }
}
