package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import io.kotest.extensions.system.withEnvironment
import io.kotest.mpp.env
import org.amshove.kluent.`should be equal to`
import org.junit.jupiter.api.Test

internal class EnvironmentTest {

    private val envVars = mapOf(
        "SERVICEUSER_USERNAME" to "username",
        "SERVICEUSER_PASSWORD" to "password",
        "NAIS_CLUSTER_NAME" to "cluster_name",
        "NAIS_NAMESPACE" to "namespace",
        "INFLUXDB_HOST" to "influxdb_host",
        "INFLUXDB_PORT" to "123",
        "INFLUXDB_DATABASE_NAME" to "influxdb_database_name",
        "INFLUXDB_USER" to "influxdb_user",
        "INFLUXDB_PASSWORD" to "influxdb_passwor",
        "INFLUXDB_RETENTION_POLICY" to "influxdb_retention_policy",
        "GROUP_ID_BASE" to "group_id_base",
        "COUNTING_INTERVAL_MINUTES" to "1",
        "KAFKA_BROKERS" to "kafka_brokers",
        "KAFKA_TRUSTSTORE_PATH" to "kafka_truststore_path",
        "KAFKA_KEYSTORE_PATH" to "kafka_keystore_path",
        "KAFKA_CREDSTORE_PASSWORD" to "kafka_credstore_password",
        "KAFKA_SCHEMA_REGISTRY" to "kafka_schema_registry",
        "KAFKA_SCHEMA_REGISTRY_USER" to "kafka_schema_registry_user",
        "KAFKA_SCHEMA_REGISTRY_PASSWORD" to "kafka_shchema_registry_password",
        "DB_USERNAME" to "db_username",
        "DB_PASSWORD" to "db_password",
        "DB_HOST" to "db_host",
        "DB_PORT" to "123",
        "DB_DATABASE" to "db_name",
        "DB_URL" to "db_url"
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
}
