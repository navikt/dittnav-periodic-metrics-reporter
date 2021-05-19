package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util

import no.nav.brukernotifikasjon.schemas.Nokkel
import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import org.apache.avro.generic.GenericRecord

object KafkaTestUtil {

    val username = "srvkafkaclient"
    val password = "kafkaclient"

    fun createDefaultKafkaEmbeddedInstance(topics: List<String>): KafkaEnvironment {
        return KafkaEnvironment(
                topicNames = topics,
                withSecurity = true,
                withSchemaRegistry = true,
                users = listOf(JAASCredential(username, password))
        )
    }

    fun createEnvironmentForEmbeddedKafka(embeddedEnv: KafkaEnvironment): Environment {
        return Environment(
                bootstrapServers = embeddedEnv.brokersURL.substringAfterLast("/"),
                schemaRegistryUrl = embeddedEnv.schemaRegistry!!.url,
                username = username,
                password = password,
                dbReadOnlyUserOnPrem = "dbAdminIkkeIBrukHer",
                dbHostOnPrem = "dbHostIkkeIBrukHer",
                dbMountPath = "dbMountPathIkkeIBrukHer",
                dbName = "dbNameIkkeIBrukHer",
                dbUrlOnPrem = "dbUrlIkkeIBrukHer",
                dbUserOnPrem = "dbUserIkkeIBrukHer",
                clusterName = "clusterNameIkkeIBrukHer",
                namespace = "namespaceIkkeIBrukHer",
                sensuHost = "sensuHostIkkeIBrukHer",
                sensuPort = 0,
                countingIntervalMinutes = 1,
                aivenBrokers = "aivenBrokersIkkeIBrukHer",
                aivenTruststorePath = "aivenTruststorePathIkkeIBrukHer",
                aivenKeystorePath = "aivenKeystorePathIkkeIBrukHer",
                aivenCredstorePassword = "aivenCredstorePasswordIkkeIBrukHer",
                aivenSchemaRegistry = "aivenSchemaRegistryIkkeIBrukHer",
                aivenSchemaRegistryUser = "aivenSchemaRegistryUser",
                aivenSchemaRegistryPassword = "aivenSchemaRegistryPassword"
        )
    }

    suspend fun <K> produceEvents(env: Environment, topicName: String, events: Map<K, GenericRecord>): Boolean {
        return KafkaProducerUtil.kafkaAvroProduce(
                env.bootstrapServers,
                env.schemaRegistryUrl,
                topicName,
                env.username,
                env.password,
                events)
    }
}
