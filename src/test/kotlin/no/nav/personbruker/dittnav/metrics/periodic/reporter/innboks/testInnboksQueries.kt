package no.nav.personbruker.dittnav.metrics.periodic.reporter.innboks

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executeBatchPersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executePersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.toBatchPersistResult
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types

private val createQuery = """INSERT INTO innboks(systembruker, eventTidspunkt, fodselsnummer, eventId, grupperingsId, tekst, link, sikkerhetsnivaa, sistOppdatert, aktiv)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

fun Connection.createInnboks(innboks: Innboks): PersistActionResult =
    executePersistQuery(createQuery) {
        buildStatementForSingleRow(innboks)
    }

fun Connection.createInnboksEventer(innboksEventer: List<Innboks>) =
    executeBatchPersistQuery(createQuery) {
        innboksEventer.forEach { innboks ->
            buildStatementForSingleRow(innboks)
            addBatch()
        }
    }.toBatchPersistResult(innboksEventer)

private fun PreparedStatement.buildStatementForSingleRow(innboks: Innboks) {
    setString(1, innboks.systembruker)
    setObject(2, innboks.eventTidspunkt, Types.TIMESTAMP)
    setString(3, innboks.fodselsnummer)
    setString(4, innboks.eventId)
    setString(5, innboks.grupperingsId)
    setString(6, innboks.tekst)
    setString(7, innboks.link)
    setInt(8, innboks.sikkerhetsnivaa)
    setObject(9, innboks.sistOppdatert, Types.TIMESTAMP)
    setBoolean(10, innboks.aktiv)
}

fun Connection.deleteAllInnboks() =
        prepareStatement("""DELETE FROM INNBOKS""").execute()
