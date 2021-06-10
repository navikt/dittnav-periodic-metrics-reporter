package no.nav.personbruker.dittnav.metrics.periodic.reporter.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.Database

class PostgresDatabase(env: Environment) : Database {

    private val envDataSource: HikariDataSource

    init {
        envDataSource = createConnectionForDb(env)
    }

    override val dataSource: HikariDataSource
        get() = envDataSource

    private fun createConnectionForDb(env: Environment): HikariDataSource {
        return hikariDataSource(env)
    }


    companion object {

        fun hikariDataSource(env: Environment): HikariDataSource {
            val config = hikariCommonConfig(env)
            config.validate()
            return HikariDataSource(config)
        }

        private fun hikariCommonConfig(env: Environment): HikariConfig {
            val config = HikariConfig().apply {
                driverClassName = "org.postgresql.Driver"
                jdbcUrl = env.dbUrl
                minimumIdle = 1
                maxLifetime = 30001
                maximumPoolSize = 5
                connectionTimeout = 4000
                validationTimeout = 1000
                idleTimeout = 10001
                isAutoCommit = false
                transactionIsolation = "TRANSACTION_REPEATABLE_READ"
                username = env.dbUser
                password = env.dbPassword
            }
            return config
        }
    }
}
