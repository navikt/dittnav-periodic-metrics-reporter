package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

data class Environment(val bootstrapServers: String = getEnvVar("KAFKA_BOOTSTRAP_SERVERS"),
                       val schemaRegistryUrl: String = getEnvVar("KAFKA_SCHEMAREGISTRY_SERVERS"),
                       val username: String = getEnvVar("SERVICEUSER_USERNAME"),
                       val password: String = getEnvVar("SERVICEUSER_PASSWORD"),
                       val dbHost: String = getEnvVar("DB_HOST"),
                       val dbName: String = getEnvVar("DB_NAME"),
                       val dbUser: String = getEnvVar("DB_NAME") + "-user",
                       val dbReadOnlyUser: String = getEnvVar("DB_NAME") + "-readonly",
                       val dbUrl: String = "jdbc:postgresql://$dbHost/$dbName",
                       val dbMountPath: String = getEnvVar("DB_MOUNT_PATH"),
                       val clusterName: String = getEnvVar("NAIS_CLUSTER_NAME"),
                       val namespace: String = getEnvVar("NAIS_NAMESPACE"),
                       val sensuHost: String = getEnvVar("SENSU_HOST"),
                       val sensuPort: String = getEnvVar("SENSU_PORT")
)

fun getEnvVar(varName: String): String {
    return System.getenv(varName)
            ?: throw IllegalArgumentException("Appen kan ikke starte uten av miljøvariabelen $varName er satt.")
}

fun isOtherEnvironmentThanProd() = System.getenv("NAIS_CLUSTER_NAME") != "prod-sbs"