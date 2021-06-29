package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import no.nav.personbruker.dittnav.common.util.config.BooleanEnvVar.getEnvVarAsBoolean
import no.nav.personbruker.dittnav.common.util.config.IntEnvVar.getEnvVarAsInt
import no.nav.personbruker.dittnav.common.util.config.LongEnvVar.getEnvVarAsLong
import no.nav.personbruker.dittnav.common.util.config.StringEnvVar.getEnvVar

data class Environment(val bootstrapServers: String = getEnvVar("KAFKA_BOOTSTRAP_SERVERS"),
                       val schemaRegistryUrl: String = getEnvVar("KAFKA_SCHEMAREGISTRY_SERVERS"),
                       val username: String = getEnvVar("SERVICEUSER_USERNAME"),
                       val password: String = getEnvVar("SERVICEUSER_PASSWORD"),
                       val dbHostOnPrem: String = getEnvVar("DB_HOST_ONPREM"),
                       val dbName: String = getEnvVar("DB_NAME"),
                       val dbUserOnPrem: String = getEnvVar("DB_NAME") + "-user",
                       val dbReadOnlyUserOnPrem: String = getEnvVar("DB_NAME") + "-readonly",
                       val dbUrlOnPrem: String = "jdbc:postgresql://$dbHostOnPrem/$dbName",
                       val dbMountPath: String = getEnvVar("DB_MOUNT_PATH"),
                       val clusterName: String = getEnvVar("NAIS_CLUSTER_NAME"),
                       val namespace: String = getEnvVar("NAIS_NAMESPACE"),
                       val sensuHost: String = getEnvVar("SENSU_HOST"),
                       val sensuPort: Int = getEnvVarAsInt("SENSU_PORT"),
                       val deltaCountingEnabled: Boolean = getEnvVarAsBoolean("DELTA_COUNTING_ENABLED", false),
                       val groupIdBase: String = "dn-periodic_metrics_reporter",
                       val countingIntervalMinutes: Long = getEnvVarAsLong("COUNTING_INTERVAL_MINUTES"),
                       val aivenBrokers: String = getEnvVar("KAFKA_BROKERS"),
                       val aivenTruststorePath: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
                       val aivenKeystorePath: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
                       val aivenCredstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
                       val aivenSchemaRegistry: String = getEnvVar("KAFKA_SCHEMA_REGISTRY"),
                       val aivenSchemaRegistryUser: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_USER"),
                       val aivenSchemaRegistryPassword: String = getEnvVar("KAFKA_SCHEMA_REGISTRY_PASSWORD"),
                       val activityHistoryLength: Int = getEnvVarAsInt("ACTIVITY_HISTORY_LENGTH", 30),
                       val lowActivityStreakThreshold: Int = getEnvVarAsInt("LOW_ACTIVITY_STREAK_THRESHOLD", 60),
                       val moderateActivityStreakThreshold: Int = getEnvVarAsInt("MODERATE_ACTIVITY_STREAK_THRESHOLD", 15),
                       val highActivityStreakThreshold: Int = getEnvVarAsInt("HIGH_ACTIVITY_STREAK_THRESHOLD", 5),
                       val monitorOnPremBeskjedActivity: Boolean = getEnvVarAsBoolean("MONITOR_ON_PREM_BESKJED_ACTIVITY"),
                       val monitorOnPremOppgaveActivity: Boolean = getEnvVarAsBoolean("MONITOR_ON_PREM_OPPGAVE_ACTIVITY"),
                       val monitorOnPremInnboksActivity: Boolean = getEnvVarAsBoolean("MONITOR_ON_PREM_INNBOKS_ACTIVITY"),
                       val monitorOnPremDoneActivity: Boolean = getEnvVarAsBoolean("MONITOR_ON_PREM_DONE_ACTIVITY"),
                       val monitorOnPremStatusoppdateringActivity: Boolean = getEnvVarAsBoolean("MONITOR_ON_PREM_STATUSOPPDATERING_ACTIVITY")
)

fun isOtherEnvironmentThanProd() = System.getenv("NAIS_CLUSTER_NAME") != "prod-sbs"
