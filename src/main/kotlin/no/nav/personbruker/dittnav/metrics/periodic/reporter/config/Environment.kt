package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.personbruker.dittnav.common.util.config.BooleanEnvVar.getEnvVarAsBoolean
import no.nav.personbruker.dittnav.common.util.config.IntEnvVar
import no.nav.personbruker.dittnav.common.util.config.LongEnvVar.getEnvVarAsLong
import no.nav.personbruker.dittnav.common.util.config.StringEnvVar.getEnvVar

data class Environment(val username: String = getEnvVar("SERVICEUSER_USERNAME"),
                       val password: String = getEnvVar("SERVICEUSER_PASSWORD"),
                       val clusterName: String = getEnvVar("NAIS_CLUSTER_NAME"),
                       val namespace: String = getEnvVar("NAIS_NAMESPACE"),
                       val deltaCountingEnabled: Boolean = getEnvVarAsBoolean("DELTA_COUNTING_ENABLED", false),
                       val groupIdBase: String = "dn-periodic_metrics_reporter",
                       val countingIntervalMinutes: Long = getEnvVarAsLong("COUNTING_INTERVAL_MINUTES"),
                       val influxdbHost: String = getEnvVar("INFLUXDB_HOST"),
                       val influxdbPort: Int = IntEnvVar.getEnvVarAsInt("INFLUXDB_PORT"),
                       val influxdbName: String = getEnvVar("INFLUXDB_DATABASE_NAME"),
                       val influxdbUser: String = getEnvVar("INFLUXDB_USER"),
                       val influxdbPassword: String = getEnvVar("INFLUXDB_PASSWORD"),
                       val influxdbRetentionPolicy: String = getEnvVar("INFLUXDB_RETENTION_POLICY"),
                       val aivenBrokers: String = getEnvVar("KAFKA_BROKERS"),
                       val aivenSchemaRegistry: String = getEnvVar("KAFKA_SCHEMA_REGISTRY"),
                       val securityConfig: SecurityConfig = SecurityConfig(isCurrentlyRunningOnNais()),
                       val requireEventsInFirstBatch : Boolean = getEnvVarAsBoolean("REQUIRE_EVENTS_IN_FIRST_BATCH", false),
                       val dbUser: String = getEnvVar("DB_USERNAME"),
                       val dbPassword: String = getEnvVar("DB_PASSWORD"),
                       val dbHost: String = getEnvVar("DB_HOST"),
                       val dbPort: Int = IntEnvVar.getEnvVarAsInt("DB_PORT"),
                       val dbName: String = getEnvVar("DB_DATABASE"),
                       val dbUrl: String = getDbUrl(dbHost, dbPort, dbName)
)

fun isOtherEnvironmentThanProd() = System.getenv("NAIS_CLUSTER_NAME") != "prod-sbs"

fun isCurrentlyRunningOnNais() = System.getenv("NAIS_APP_NAME") != null

data class SecurityConfig(
    val enabled: Boolean,

    val variables: SecurityVars? = if (enabled) {
        SecurityVars()
    } else {
        null
    }
)

data class SecurityVars(
    val aivenTruststorePath: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
    val aivenKeystorePath: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
    val aivenCredstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
    val aivenSchemaRegistryUser: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_USER"),
    val aivenSchemaRegistryPassword: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_PASSWORD")
)

fun getDbUrl(host: String, port: Int, name: String): String {
    return "jdbc:postgresql://${host}:${port}/${name}"
}
