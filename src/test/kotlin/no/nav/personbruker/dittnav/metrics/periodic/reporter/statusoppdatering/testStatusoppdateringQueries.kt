import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.PersistActionResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executeBatchPersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.executePersistQuery
import no.nav.personbruker.dittnav.metrics.periodic.reporter.common.database.util.toBatchPersistResult
import no.nav.personbruker.dittnav.metrics.periodic.reporter.statusoppdatering.Statusoppdatering
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types

private val createQuery = """INSERT INTO statusoppdatering (systembruker, eventId, eventTidspunkt, fodselsnummer, grupperingsId, link, sikkerhetsnivaa, sistOppdatert, statusGlobal, statusIntern, sakstema)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

fun Connection.createStatusoppdatering(statusoppdatering: Statusoppdatering): PersistActionResult =
    executePersistQuery(createQuery) {
        buildStatementForSingleRow(statusoppdatering)
    }

fun Connection.createStatusoppdateringer(statusoppdateringer: List<Statusoppdatering>) =
    executeBatchPersistQuery(createQuery) {
        statusoppdateringer.forEach { statusoppdatering ->
            buildStatementForSingleRow(statusoppdatering)
            addBatch()
        }
    }.toBatchPersistResult(statusoppdateringer)

private fun PreparedStatement.buildStatementForSingleRow(statusoppdatering: Statusoppdatering) {
    setString(1, statusoppdatering.systembruker)
    setString(2, statusoppdatering.eventId)
    setObject(3, statusoppdatering.eventTidspunkt, Types.TIMESTAMP)
    setString(4, statusoppdatering.fodselsnummer)
    setString(5, statusoppdatering.grupperingsId)
    setString(6, statusoppdatering.link)
    setInt(7, statusoppdatering.sikkerhetsnivaa)
    setObject(8, statusoppdatering.sistOppdatert, Types.TIMESTAMP)
    setString(9, statusoppdatering.statusGlobal)
    setString(10, statusoppdatering.statusIntern)
    setString(11, statusoppdatering.sakstema)
}

fun Connection.deleteAllStatusoppdatering() =
    prepareStatement("""DELETE FROM STATUSOPPDATERING""")
        .use {it.execute()}
