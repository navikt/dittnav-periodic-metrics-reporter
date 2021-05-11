package no.nav.personbruker.dittnav.metrics.periodic.reporter.done

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executeBatchPersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.toBatchPersistResult
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types

private const val createQuery = """INSERT INTO done(systembruker, eventTidspunkt, fodselsnummer, eventId, grupperingsId)
            VALUES (?, ?, ?, ?, ?)"""

fun Connection.deleteAllDone() =
        prepareStatement("""DELETE FROM DONE""")
                .use {it.execute()}

fun Connection.createDoneEvents(doneEvents: List<Done>): ListPersistActionResult<Done> =
        executeBatchPersistQuery(createQuery) {
                doneEvents.forEach { done ->
                        buildStatementForSingleRow(done)
                        addBatch()
                }
        }.toBatchPersistResult(doneEvents)

private fun PreparedStatement.buildStatementForSingleRow(doneEvent: Done) {
        setString(1, doneEvent.systembruker)
        setObject(2, doneEvent.eventTidspunkt, Types.TIMESTAMP)
        setString(3, doneEvent.fodselsnummer)
        setString(4, doneEvent.eventId)
        setString(5, doneEvent.grupperingsId)
}
