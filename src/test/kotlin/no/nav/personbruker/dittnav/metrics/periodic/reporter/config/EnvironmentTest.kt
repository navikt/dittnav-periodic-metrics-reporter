package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import io.kotest.extensions.system.withEnvironment
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Test

internal class EnvironmentTest {

    private val envVars = mapOf(
        "KAFKA_BOOTSTRAP_SERVERS" to "bootstrap_servers",
        "KAFKA_SCHEMAREGISTRY_SERVERS" to "schemaregistry_servers",
        "SERVICEUSER_USERNAME" to "username",
        "SERVICEUSER_PASSWORD" to "password",
        "DB_HOST_ONPREM" to "db_host",
        "DB_NAME" to "db_name",
        "DB_MOUNT_PATH" to "db_mount_path",
        "NAIS_CLUSTER_NAME" to "cluster_name",
        "NAIS_NAMESPACE" to "namespace",
        "SENSU_HOST" to "sensu_host",
        "SENSU_PORT" to "1",
        "COUNTING_INTERVAL_MINUTES" to "1",
        "KAFKA_BROKERS" to "kafka_brokers",
        "KAFKA_TRUSTSTORE_PATH" to "kafka_truststore_path",
        "KAFKA_KEYSTORE_PATH" to "kafka_keystore_path",
        "KAFKA_CREDSTORE_PASSWORD" to "kafka_credstore_password",
        "KAFKA_SCHEMA_REGISTRY" to "kafka_schema_registry",
        "KAFKA_SCHEMA_REGISTRY_USER" to "kafka_schema_registry_user",
        "KAFKA_SCHEMA_REGISTRY_PASSWORD" to "kafka_shchema_registry_password",
        "MONITOR_ON_PREM_BESKJED_ACTIVITY" to "true",
        "MONITOR_ON_PREM_OPPGAVE_ACTIVITY" to "true",
        "MONITOR_ON_PREM_INNBOKS_ACTIVITY" to "true",
        "MONITOR_ON_PREM_DONE_ACTIVITY" to "true",
        "MONITOR_ON_PREM_STATUSOPPDATERING_ACTIVITY" to "true"
    )

    @Test
    fun `Om DELTA_COUNTING_MODE ikke er satt som env_var evalueres den til default false`() {
        withEnvironment(envVars) {
            Environment().deltaCountingEnabled `should be equal to` false
        }
    }

    @Test
    fun `Om DELTA_COUNTING_MODE  er satt som "FALSE" env_var evalueres den til false`() {
        withEnvironment(envVars + ("DELTA_COUNTING_ENABLED" to "false")) {
            Environment().deltaCountingEnabled `should be equal to` false
        }
    }

    @Test
    fun `Om DELTA_COUNTING_MODE er satt som "TRUE" env_var evalueres den til true`() {
        withEnvironment(envVars + ("DELTA_COUNTING_ENABLED" to "true")) {
            Environment().deltaCountingEnabled `should be equal to` true
        }
    }

    @Test
    fun `DB_HOST og DB_NAME benyttes til utledning av dbUser, dbReadonly, dbUrl`() {
        withEnvironment(envVars) {
            Environment().dbUserOnPrem `should be equal to`  "db_name-user"
            Environment().dbReadOnlyUserOnPrem `should be equal to`  "db_name-readonly"
            Environment().dbUrlOnPrem `should be equal to`  "jdbc:postgresql://db_host/db_name"
        }
    }
}
