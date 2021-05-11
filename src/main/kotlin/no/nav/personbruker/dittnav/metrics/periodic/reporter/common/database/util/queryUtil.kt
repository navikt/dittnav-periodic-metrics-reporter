package no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistFailureReason
import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import java.sql.*

const val countResultColumnIndex = 1

fun <T> ResultSet.singleResult(result: ResultSet.() -> T): T =
        if (next()) {
            result()
        } else {
            throw SQLException("Found no rows")
        }

fun <T> ResultSet.list(result: ResultSet.() -> T): List<T> =
        mutableListOf<T>().apply {
            while (next()) {
                add(result())
            }
        }

fun Connection.executeBatchPersistQuery(sql: String, paramInit: PreparedStatement.() -> Unit): IntArray {
    autoCommit = false
    val result = prepareStatement("""$sql ON CONFLICT DO NOTHING""").use { statement ->
        statement.paramInit()
        statement.executeBatch()
    }
    commit()
    return result
}

fun <T> IntArray.toBatchPersistResult(paramList: List<T>) = ListPersistActionResult.mapParamListToResultArray(paramList, this)


fun Connection.executePersistQuery(sql: String, paramInit: PreparedStatement.() -> Unit): PersistActionResult =
        prepareStatement("""$sql ON CONFLICT DO NOTHING""", Statement.RETURN_GENERATED_KEYS).use {

            it.paramInit()
            it.executeUpdate()

            if (it.generatedKeys.next()) {
                PersistActionResult.success(it.generatedKeys.getInt("id"))
            } else {
                PersistActionResult.failure(PersistFailureReason.CONFLICTING_KEYS)
            }
        }

fun Connection.countTotalNumberOfEvents(eventType: EventType): Long {
    val numberOfEvents = prepareStatement("SELECT count(*) from $eventType",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)
            .use { statement ->
                val resultSet = statement.executeQuery()
                resultSet.last()
                resultSet.getLong(countResultColumnIndex)
            }
    return numberOfEvents
}

fun Connection.countTotalNumberOfEventsByActiveStatus(eventType: EventType, aktiv: Boolean): Long {
    val numberOfEvents = prepareStatement("SELECT count(*) from $eventType where aktiv = ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)
            .use { statement ->
                statement.setBoolean(1, aktiv)
                val resultSet = statement.executeQuery()
                resultSet.last()
                resultSet.getLong(countResultColumnIndex)
            }
    return numberOfEvents
}
