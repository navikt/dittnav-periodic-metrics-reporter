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
        "DB_HOST" to "db_host",
        "DB_NAME" to "db_name",
        "DB_MOUNT_PATH" to "db_mount_path",
        "NAIS_CLUSTER_NAME" to "cluster_name",
        "NAIS_NAMESPACE" to "namespace",
        "SENSU_HOST" to "sensu_host",
        "SENSU_PORT" to "sensu_port"
    )

    @Test
    fun `Om DELTA_COUNTING_MODE ikke er satt som env_var evalueres den til default false`() {
        withEnvironment(envVars) {
            Environment().deltaCountingModeActive `should be equal to` false
        }
    }

    @Test
    fun `Om DELTA_COUNTING_MODE  er satt som "FALSE" env_var evalueres den til false`() {
        withEnvironment(envVars + ("DELTA_COUNTING_MODE" to "false")) {
            Environment().deltaCountingModeActive `should be equal to` false
        }
    }



    @Test
    fun `Om DELTA_COUNTING_MODE er satt som "TRUE" env_var evalueres den til true`() {
        withEnvironment(envVars + ("DELTA_COUNTING_MODE" to "true")) {
            Environment().deltaCountingModeActive `should be equal to` true
        }
    }


    @Test
    fun `DB_HOST og DB_NAME benyttes til utledning av dbUser, dbReadonly, dbUrl`() {
        withEnvironment(envVars) {
            Environment().dbUser `should be equal to`  "db_name-user"
            Environment().dbReadOnlyUser `should be equal to`  "db_name-readonly"
            Environment().dbUrl `should be equal to`  "jdbc:postgresql://db_host/db_name"
        }
    }
}