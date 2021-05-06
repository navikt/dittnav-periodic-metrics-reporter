package no.nav.personbruker.dittnav.metrics.periodic.reporter.beskjed

import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.ListPersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executeBatchPersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executePersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.toBatchPersistResult
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types

private val createQuery = """INSERT INTO beskjed (uid, systembruker, eventTidspunkt, fodselsnummer, eventId, grupperingsId, tekst, link, sikkerhetsnivaa, sistOppdatert, synligFremTil, aktiv)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

fun Connection.createBeskjed(beskjed: Beskjed): PersistActionResult =
    executePersistQuery(createQuery) {
        buildStatementForSingleRow(beskjed)
    }

fun Connection.deleteAllBeskjed() =
        prepareStatement("""DELETE FROM BESKJED""")
                .use {it.execute()}

fun Connection.deleteBeskjedWithEventId(eventId: String) =
        prepareStatement("""DELETE FROM BESKJED WHERE eventId = ?""")
                .use {
                    it.setString(1, eventId)
                    it.execute()
                }

fun Connection.createBeskjeder(beskjeder: List<Beskjed>): ListPersistActionResult<Beskjed> =
    executeBatchPersistQuery(createQuery) {
        beskjeder.forEach { beskjed ->
            buildStatementForSingleRow(beskjed)
            addBatch()
        }
    }.toBatchPersistResult(beskjeder)

private fun PreparedStatement.buildStatementForSingleRow(beskjed: Beskjed) {
    setString(1, beskjed.uid)
    setString(2, beskjed.systembruker)
    setObject(3, beskjed.eventTidspunkt, Types.TIMESTAMP)
    setString(4, beskjed.fodselsnummer)
    setString(5, beskjed.eventId)
    setString(6, beskjed.grupperingsId)
    setString(7, beskjed.tekst)
    setString(8, beskjed.link)
    setInt(9, beskjed.sikkerhetsnivaa)
    setObject(10, beskjed.sistOppdatert, Types.TIMESTAMP)
    setObject(11, beskjed.synligFremTil, Types.TIMESTAMP)
    setBoolean(12, beskjed.aktiv)
}
