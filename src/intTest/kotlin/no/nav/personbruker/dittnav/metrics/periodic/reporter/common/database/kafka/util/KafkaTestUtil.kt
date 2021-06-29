package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.kafka.util

import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.Environment
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
                aivenBrokers = embeddedEnv.brokersURL.substringAfterLast("/"),
                aivenTruststorePath = "aivenTruststorePathIkkeIBrukHer",
                aivenKeystorePath = "aivenKeystorePathIkkeIBrukHer",
                aivenCredstorePassword = "aivenCredstorePasswordIkkeIBrukHer",
                aivenSchemaRegistry = embeddedEnv.schemaRegistry!!.url,
                aivenSchemaRegistryUser = username,
                aivenSchemaRegistryPassword = password,
                lowActivityStreakThreshold = 60,
                moderateActivityStreakThreshold = 15,
                highActivityStreakThreshold = 5,
                monitorOnPremBeskjedActivity = false,
                monitorOnPremOppgaveActivity = false,
                monitorOnPremInnboksActivity = false,
                monitorOnPremDoneActivity = false,
                monitorOnPremStatusoppdateringActivity = false

        )
    }

    suspend fun <K> produceEvents(env: Environment, topicName: String, enableSecurity: Boolean, events: Map<K, GenericRecord>): Boolean {
        return KafkaProducerUtil.kafkaAvroProduce(
                env.bootstrapServers,
                env.schemaRegistryUrl,
                topicName,
                env.username,
                env.password,
                enableSecurity,
                events)
    }
}
