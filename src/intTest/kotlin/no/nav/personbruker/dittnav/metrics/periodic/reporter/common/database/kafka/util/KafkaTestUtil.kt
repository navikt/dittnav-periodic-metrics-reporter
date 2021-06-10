package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util

import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.SecurityConfig
import org.apache.avro.generic.GenericRecord

object KafkaTestUtil {

    val username = "srvkafkaclient"
    val password = "kafkaclient"

    fun createDefaultKafkaEmbeddedInstance(withSecurity: Boolean, topics: List<String>): KafkaEnvironment {
        return KafkaEnvironment(
                topicNames = topics,
                withSecurity = withSecurity,
                withSchemaRegistry = true,
                users = listOf(JAASCredential(username, password))
        )
    }

    fun createEnvironmentForEmbeddedKafka(embeddedEnv: KafkaEnvironment): Environment {
        return Environment(
            username = username,
            password = password,
            dbHost = "dbHostIkkeIBrukHer",
            dbPort = 12,
            dbName = "dbNameIkkeIBrukHer",
            dbUrl = "dbUrlIkkeIBrukHer",
            dbUser = "dbUserIkkeIBrukHer",
            dbPassword = "dbPWIkkeIBrukHer",
            clusterName = "clusterNameIkkeIBrukHer",
            namespace = "namespaceIkkeIBrukHer",
            aivenBrokers = embeddedEnv.brokersURL.substringAfterLast("/"),
            aivenSchemaRegistry = embeddedEnv.schemaRegistry!!.url,
            securityConfig = SecurityConfig(enabled = false),
            countingIntervalMinutes = 1,
            influxdbHost = "influxdbHostIkkeIBrukHer",
            influxdbPort = 12,
            influxdbName = "influxdbNameIkkeIBrukHer",
            influxdbUser = "influxdbUserIkkeIBrukHer",
            influxdbPassword = "influxdbPasswordIkkeIBrukHer",
            influxdbRetentionPolicy = "influxdbRetentionPolicyIkkeIBrukHer"
        )
    }

    suspend fun <K> produceEvents(env: Environment, topicName: String, events: Map<K, GenericRecord>): Boolean {
        return KafkaProducerUtil.kafkaAvroProduce(
                env.aivenBrokers,
                env.aivenSchemaRegistry,
                topicName,
                events)
    }
}
