package no.nav.personbruker.dittnav.metrics.periodic.reporter.metrics.db.count

import no.nav.personbruker.dittnav.metrics.periodic.reporter.config.EventType
import java.sql.Connection
import java.sql.ResultSet

const val countResultColumnIndex = 1

fun Connection.countTotalNumberOfEvents(eventType: EventType): Long {
    return prepareStatement("SELECT COUNT(*) FROM $eventType",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)
            .use { statement ->
                val resultSet = statement.executeQuery()
                resultSet.last()
                resultSet.getLong(countResultColumnIndex)
            }
}

fun Connection.countTotalNumberOfEventsByActiveStatus(eventType: EventType, aktiv: Boolean): Long {
    return prepareStatement("SELECT COUNT(*) FROM $eventType WHERE aktiv = ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)
            .use { statement ->
                statement.setBoolean(1, aktiv)
                val resultSet = statement.executeQuery()
                resultSet.last()
                resultSet.getLong(countResultColumnIndex)
            }
}

fun Connection.countTotalNumberOfBrukernotifikasjonerByActiveStatus(aktiv: Boolean): Map<String, Int> {
    return prepareStatement(
"""SELECT
                subquery.systembruker, sum(count)
        FROM (
             SELECT systembruker, COUNT(1) as count FROM BESKJED WHERE aktiv = ? GROUP BY systembruker
             UNION ALL
             SELECT systembruker, COUNT(1) as count FROM OPPGAVE WHERE aktiv = ? GROUP BY systembruker
             UNION ALL
             SELECT systembruker, COUNT(1) as count FROM INNBOKS WHERE aktiv = ? GROUP BY systembruker
        ) as subquery group by subquery.systembruker order by subquery.systembruker;
    """,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)
            .use { statement ->
                statement.setBoolean(1, aktiv)
                statement.setBoolean(2, aktiv)
                statement.setBoolean(3, aktiv)
                val resultSet = statement.executeQuery()
                mutableMapOf<String, Int>().apply {
                    while (resultSet.next()) {
                        put(resultSet.getString(1), resultSet.getInt(2))
                    }
                }
            }
}

fun Connection.countTotalNumberOfEventsGroupedBySystembruker(eventType: EventType): Map<String, Int> {
    return prepareStatement("SELECT systembruker, COUNT(*) FROM $eventType GROUP BY systembruker",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY)
            .use { statement ->
                val resultSet = statement.executeQuery()
                mutableMapOf<String, Int>().apply {
                    while (resultSet.next()) {
                        put(resultSet.getString(1), resultSet.getInt(2))
                    }
                }
            }
}
