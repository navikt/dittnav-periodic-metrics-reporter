package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.personbruker.dittnav.common.util.config.StringEnvVar.getEnvVar
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database
import no.nav.vault.jdbc.hikaricp.HikariCPVaultUtil

class PostgresDatabase(env: Environment) : Database {

    private val envDataSource: HikariDataSource

    init {
        envDataSource = createCorrectConnectionForEnvironment(env)
    }

    override val dataSource: HikariDataSource
        get() = envDataSource

    private fun createCorrectConnectionForEnvironment(env: Environment): HikariDataSource {
        return when (ConfigUtil.isCurrentlyRunningOnNais()) {
            true -> createConnectionViaVaultWithDbUser(env)
            false -> createConnectionForLocalDbWithDbUser(env)
        }
    }

    private fun createConnectionForLocalDbWithDbUser(env: Environment): HikariDataSource {
        return hikariFromLocalDb(env, env.dbUserOnPrem)
    }

    private fun createConnectionViaVaultWithDbUser(env: Environment): HikariDataSource {
        return hikariDatasourceViaVault(env, env.dbReadOnlyUserOnPrem)
    }

    companion object {

        fun hikariFromLocalDb(env: Environment, dbUser: String): HikariDataSource {
            val dbPassword: String = getEnvVar("DB_PASSWORD")
            val config = hikariCommonConfig(env).apply {
                username = dbUser
                password = dbPassword
                validate()
            }
            return HikariDataSource(config)
        }

        fun hikariDatasourceViaVault(env: Environment, dbUser: String): HikariDataSource {
            val config = hikariCommonConfig(env)
            config.validate()
            return HikariCPVaultUtil.createHikariDataSourceWithVaultIntegration(config, env.dbMountPath, dbUser)
        }

        private fun hikariCommonConfig(env: Environment): HikariConfig {
            val config = HikariConfig().apply {
                driverClassName = "org.postgresql.Driver"
                jdbcUrl = env.dbUrlOnPrem
                minimumIdle = 1
                maxLifetime = 30001
                maximumPoolSize = 5
                connectionTimeout = 4000
                validationTimeout = 1000
                idleTimeout = 10001
                isAutoCommit = false
                transactionIsolation = "TRANSACTION_REPEATABLE_READ"
            }
            return config
        }
    }
}
