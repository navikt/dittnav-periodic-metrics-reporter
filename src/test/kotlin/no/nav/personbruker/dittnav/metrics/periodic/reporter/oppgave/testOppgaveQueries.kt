package no.nav.personbruker.dittnav.metrics.periodic.reporter.oppgave

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executeBatchPersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executePersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.toBatchPersistResult
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types

private val createQuery = """INSERT INTO oppgave (systembruker, eventTidspunkt, fodselsnummer, eventId, grupperingsId, tekst, link, sikkerhetsnivaa, sistOppdatert, aktiv) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,?)"""

fun Connection.createOppgaver(oppgaver: List<Oppgave>) =
    executeBatchPersistQuery(createQuery) {
        oppgaver.forEach { oppgave ->
            buildStatementForSingleRow(oppgave)
            addBatch()
        }
    }.toBatchPersistResult(oppgaver)

fun Connection.createOppgave(oppgave: Oppgave): PersistActionResult =
    executePersistQuery(createQuery) {
        buildStatementForSingleRow(oppgave)
        addBatch()
    }

private fun PreparedStatement.buildStatementForSingleRow(oppgave: Oppgave) {
    setString(1, oppgave.systembruker)
    setObject(2, oppgave.eventTidspunkt, Types.TIMESTAMP)
    setString(3, oppgave.fodselsnummer)
    setString(4, oppgave.eventId)
    setString(5, oppgave.grupperingsId)
    setString(6, oppgave.tekst)
    setString(7, oppgave.link)
    setInt(8, oppgave.sikkerhetsnivaa)
    setObject(9, oppgave.sistOppdatert, Types.TIMESTAMP)
    setBoolean(10, oppgave.aktiv)
}

fun Connection.deleteAllOppgave() =
        prepareStatement("""DELETE FROM OPPGAVE""")
                .use {it.execute()}
